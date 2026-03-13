import fs from 'fs/promises';
import path from 'path';
import logger from '../../utils/logger.js';
import { ABSApiClient } from './ABSApiClient.js';
import { DataFlowService } from './DataFlowService.js';
import {
    AvailabilityGuidance,
    CodeItem,
    ConceptMetadata,
    DataFlow,
    DatasetAvailabilityMap,
    DataStructureMetadata,
    DimensionAvailabilitySummary,
    DimensionAvailabilityValue,
    DimensionMetadata,
    DimensionValueSummary
} from '../../types/abs.js';

interface AvailabilityOptions {
    forceRefresh?: boolean;
}

const SAMPLE_VALUE_LIMIT = 40;
const MISSING_VALUE_LIMIT = 20;
const RELATION_DIMENSION_LIMIT = 48;
const AGGREGATE_CODE_CANDIDATES = new Set(['TOT', 'TOTAL', '_T', 'ALL', 'IND', 'AUS']);

type RelationStats = Map<string, Map<string, Map<string, Set<string>>>>;

interface CodeLookupEntry {
    codes: Map<string, CodeItem>;
    total: number;
}

interface MetadataLookupContext {
    dimensionIndex: Map<string, DimensionMetadata>;
    conceptIndex: Map<string, ConceptMetadata>;
    codeLookup: Map<string, CodeLookupEntry>;
}

export class DatasetAvailabilityService {
    private readonly apiClient: ABSApiClient;

    constructor(
        private readonly dataFlowService: DataFlowService,
        private readonly availabilityCacheDir: string
    ) {
        this.apiClient = new ABSApiClient();
    }

    async getAvailabilityMap(datasetId: string, options: AvailabilityOptions = {}): Promise<DatasetAvailabilityMap> {
        await this.ensureCacheDir();
        const forceRefresh = options.forceRefresh ?? false;

        const flow = await this.dataFlowService.resolveFlow(datasetId, forceRefresh);
        const datasetKey = DataFlowService.formatDataflowIdentifier(flow);
        const cachePath = this.buildCachePath(datasetKey);

        if (!forceRefresh) {
            const cached = await this.readCache(cachePath);
            if (cached) {
                return cached;
            }
        }

        const metadata = await this.dataFlowService.getDataStructureForDataflow(datasetKey, forceRefresh);
        const payload = await this.apiClient.getData(datasetKey, 'all', {
            format: 'jsondata',
            detail: 'dataonly',
            dimensionAtObservation: 'TIME_PERIOD',
            lastNObservations: 1
        });

        const availabilityMap = this.composeAvailabilityMap(flow, metadata, payload);
        await this.writeCache(cachePath, availabilityMap);
        return availabilityMap;
    }

    private composeAvailabilityMap(
        flow: DataFlow,
        metadata: DataStructureMetadata,
        payload: any
    ): DatasetAvailabilityMap {
        if (!payload?.data) {
            throw new Error('ABS response did not contain data payload for availability map generation');
        }

        const structure = this.toArray(payload.data.structures)[0];
        if (!structure) {
            throw new Error('ABS response did not include structure metadata required for availability map');
        }

        const seriesDimensions = this.toArray(structure.dimensions?.series ?? []);
        const dataset = (this.toArray(payload.data.dataSets)[0] ?? {}) as any;
        const seriesEntries = Object.entries<any>(dataset.series ?? {});

        let activeDimensions = seriesDimensions;
        let activeEntries = seriesEntries;
        if (seriesEntries.length === 0) {
            activeDimensions = this.toArray(structure.dimensions?.observation ?? []);
            activeEntries = Object.entries<any>(dataset.observations ?? {});
        }

        const dimensionOrder = activeDimensions.map((dim: any, index: number) => dim?.id ?? `DIM_${index}`);
        const combinations = new Set<string>();
        const dimensionValueSets = new Map<string, Set<string>>();
        const codeFrequencies = new Map<string, Map<string, number>>();
        const relationTargets = new Set<string>(dimensionOrder);
        const relationStats: RelationStats = new Map();

        for (const [key] of activeEntries) {
            const indices = this.parseKeyIndices(key, activeDimensions.length);
            const coordinates = this.buildCoordinateRecord(activeDimensions, indices);
            const parts: string[] = [];

            dimensionOrder.forEach((dimensionId) => {
                const value = coordinates[dimensionId];
                const code = (value?.code ?? '').toString().toUpperCase();
                const registry = dimensionValueSets.get(dimensionId) ?? new Set<string>();
                registry.add(code);
                dimensionValueSets.set(dimensionId, registry);
                if (registry.size > RELATION_DIMENSION_LIMIT && relationTargets.has(dimensionId)) {
                    relationTargets.delete(dimensionId);
                    relationStats.delete(dimensionId);
                }

                const frequencyMap = codeFrequencies.get(dimensionId) ?? new Map<string, number>();
                frequencyMap.set(code, (frequencyMap.get(code) ?? 0) + 1);
                codeFrequencies.set(dimensionId, frequencyMap);

                parts.push(`${dimensionId}=${code}`);
            });

            combinations.add(parts.join('|'));

            if (relationTargets.size > 0) {
                this.updateRelationStats(relationTargets, relationStats, coordinates, dimensionOrder);
            }
        }

        if (combinations.size === 0) {
            throw new Error('ABS returned no series combinations for availability map generation');
        }

        const fixedDimensions: Record<string, string> = {};
        dimensionValueSets.forEach((values, dimensionId) => {
            if (values.size === 1) {
                const [only] = Array.from(values);
                fixedDimensions[dimensionId] = only;
            }
        });

        const metadataLookups = this.buildMetadataLookups(metadata);
        const dimensionAvailability = this.buildDimensionAvailability(
            dimensionOrder,
            dimensionValueSets,
            codeFrequencies,
            fixedDimensions,
            metadataLookups
        );
        const guidance = this.buildGuidance(dimensionAvailability, relationStats, metadataLookups.codeLookup);

        const availability: DatasetAvailabilityMap = {
            datasetId: DataFlowService.formatDataflowIdentifier(flow),
            dataflow: flow,
            generatedAt: new Date().toISOString(),
            totalSeries: combinations.size,
            dimensionOrder,
            dimensionAvailability,
            guidance
        };

        if (Object.keys(fixedDimensions).length > 0) {
            availability.fixedDimensions = fixedDimensions;
        }

        return availability;
    }

    private parseKeyIndices(key: string, expectedLength: number): number[] {
        if (expectedLength === 0) {
            return [];
        }
        const parts = typeof key === 'string' ? key.split(':') : [];
        const indices = parts
            .filter((part) => part !== '')
            .map((part) => {
                const numeric = Number(part);
                return Number.isNaN(numeric) ? 0 : numeric;
            });
        while (indices.length < expectedLength) {
            indices.push(0);
        }
        return indices.slice(0, expectedLength);
    }

    private buildCoordinateRecord(dimensions: any[], indices: number[]): Record<string, DimensionValueSummary> {
        const record: Record<string, DimensionValueSummary> = {};
        dimensions.forEach((dim, idx) => {
            const index = indices[idx] ?? 0;
            const dimensionId = dim?.id ?? `DIM_${idx}`;
            const valueMeta = this.toArray(dim?.values)[index];
            record[dimensionId] = {
                code: valueMeta?.id ?? String(index),
                label: this.extractName(valueMeta),
                description: this.extractDescription(valueMeta)
            };
        });
        return record;
    }

    private toArray<T>(value: T | T[] | undefined | null): T[] {
        if (Array.isArray(value)) {
            return value;
        }
        if (value === undefined || value === null) {
            return [];
        }
        return [value];
    }

    private extractName(value: any): string | undefined {
        if (value === null || value === undefined) {
            return undefined;
        }
        if (typeof value === 'string') {
            return value;
        }
        if (typeof value === 'number') {
            return value.toString();
        }
        if (value?.name && typeof value.name === 'object') {
            return value.name.en ?? Object.values(value.name)[0];
        }
        if (typeof value?.name === 'string') {
            return value.name;
        }
        if (typeof value?._text === 'string') {
            return value._text;
        }
        return undefined;
    }

    private extractDescription(value: any): string | undefined {
        if (!value) {
            return undefined;
        }
        if (typeof value?.description === 'string') {
            return value.description;
        }
        if (value?.description && typeof value.description === 'object') {
            return value.description.en ?? Object.values(value.description)[0];
        }
        return undefined;
    }

    private buildMetadataLookups(metadata: DataStructureMetadata): MetadataLookupContext {
        const dimensionIndex = new Map<string, DimensionMetadata>();
        this.toArray(metadata.dimensions).forEach((dimension) => {
            if (dimension?.id) {
                dimensionIndex.set(dimension.id, dimension);
            }
        });

        const conceptIndex = new Map<string, ConceptMetadata>();
        this.toArray(metadata.concepts).forEach((concept) => {
            if (concept?.id) {
                conceptIndex.set(concept.id, concept);
            }
        });

        const codeLookup = new Map<string, CodeLookupEntry>();
        const codelistIndex = new Map<string, CodeItem[]>();
        this.toArray(metadata.codelists).forEach((codelist) => {
            if (codelist?.id) {
                codelistIndex.set(codelist.id, this.toArray(codelist.codes));
            }
        });

        dimensionIndex.forEach((dimension, dimensionId) => {
            const listId = dimension?.codelist?.id;
            const codes = listId ? codelistIndex.get(listId) ?? [] : [];
            const codeMap = new Map<string, CodeItem>();
            codes.forEach((code) => {
                if (code?.id) {
                    codeMap.set(code.id, code);
                }
            });
            codeLookup.set(dimensionId, { codes: codeMap, total: codes.length });
        });

        return { dimensionIndex, conceptIndex, codeLookup };
    }

    private buildDimensionAvailability(
        dimensionOrder: string[],
        dimensionValueSets: Map<string, Set<string>>,
        codeFrequencies: Map<string, Map<string, number>>,
        fixedDimensions: Record<string, string>,
        lookups: MetadataLookupContext
    ): Record<string, DimensionAvailabilitySummary> {
        const availability: Record<string, DimensionAvailabilitySummary> = {};

        for (const dimensionId of dimensionOrder) {
            const observedValues = dimensionValueSets.get(dimensionId) ?? new Set<string>();
            const frequencyMap = codeFrequencies.get(dimensionId) ?? new Map<string, number>();
            const fixedValue = fixedDimensions[dimensionId];
            const codeLookupEntry = lookups.codeLookup.get(dimensionId);
            const dimensionMeta = lookups.dimensionIndex.get(dimensionId);
            const concept = dimensionMeta?.conceptId ? lookups.conceptIndex.get(dimensionMeta.conceptId) : undefined;

            const { values, limitHit } = this.buildValueSamples(frequencyMap, codeLookupEntry?.codes ?? new Map());
            const missingCodes = this.buildMissingSamples(observedValues, codeLookupEntry?.codes ?? new Map());

            const summary: DimensionAvailabilitySummary = {
                dimensionId,
                label: concept?.name || dimensionId,
                description: concept?.description,
                required: true,
                supportsMultiSelect: observedValues.size > 1,
                observedValueCount: observedValues.size,
                totalValueCount: codeLookupEntry?.total,
                sampleSize: values.length,
                values,
                valueLimitHit: limitHit || undefined,
                missingCodes: missingCodes.length ? missingCodes : undefined,
                missingLimitHit:
                    missingCodes.length > 0 && (codeLookupEntry?.total ?? 0) > missingCodes.length ? true : undefined,
                fixedValue,
                notes: this.buildDimensionNotes(
                    dimensionId,
                    observedValues.size,
                    fixedValue,
                    limitHit,
                    missingCodes.length,
                    codeLookupEntry?.codes ?? new Map()
                )
            };

            availability[dimensionId] = summary;
        }

        return availability;
    }

    private buildGuidance(
        dimensionAvailability: Record<string, DimensionAvailabilitySummary>,
        relationStats: RelationStats,
        codeLookup: Map<string, CodeLookupEntry>
    ): AvailabilityGuidance {
        const general: string[] = [
            'Provide `dimensionFilters` entries for every listed dimension using arrays of SDMX codes (even for single selections).',
            'Use `startPeriod`/`endPeriod` to control the time window; `TIME_PERIOD` remains at the observation level in the resolver.',
            'Default to aggregates (AUS, TOTAL, `_T`) before drilling into detailed geography/industry unless the planner explicitly requests depth.'
        ];

        if (Object.values(dimensionAvailability).some((entry) => entry.supportsMultiSelect)) {
            general.push('Multi-select is supported—list multiple codes per dimension to compare industries/products in one request.');
        }

        const compatibilityHints = [
            ...this.generateStructuralHints(dimensionAvailability),
            ...this.buildRelationHints(relationStats, dimensionAvailability, codeLookup)
        ];

        return {
            general,
            compatibilityHints
        };
    }

    private generateStructuralHints(
        dimensionAvailability: Record<string, DimensionAvailabilitySummary>
    ): string[] {
        const hints: string[] = [];
        if (dimensionAvailability['REGION_TYPE'] && dimensionAvailability['REGION']) {
            const observed = dimensionAvailability['REGION_TYPE'].values
                .map((value) => value.label ? `${value.code} (${value.label})` : value.code)
                .join(', ');
            hints.push(
                `REGION_TYPE selects the geographic granularity for REGION (observed: ${observed}). Choose the level first, then only provide REGION codes from that level.`
            );
        }

        if (dimensionAvailability['STATE'] && dimensionAvailability['REGION']) {
            hints.push(
                'STATE codes (1=NSW … 8=ACT, AUS=Australia) must align with REGION. Always include STATE whenever you drill below national totals.'
            );
        }

        if (dimensionAvailability['PRODUCT'] && dimensionAvailability['INDUSTRY']) {
            hints.push(
                'Supply-use tables require both PRODUCT and INDUSTRY selections. Use arrays for each dimension when comparing multiple items, and keep the lists short to avoid huge slices.'
            );
        }

        if (dimensionAvailability['FLOW']) {
            hints.push(
                'FLOW distinguishes supply vs use (and similar analytical families). Pick the FLOW value that matches the requested concept before layering additional dimensions.'
            );
        }

        if (dimensionAvailability['MEASURE']) {
            hints.push(
                'MEASURE controls the metric (e.g., persons, rates, dollar value). Stabilize MEASURE before adjusting other dimensions to avoid ambiguous requests.'
            );
        }

        return hints;
    }

    private buildRelationHints(
        relationStats: RelationStats,
        dimensionAvailability: Record<string, DimensionAvailabilitySummary>,
        codeLookup: Map<string, CodeLookupEntry>
    ): string[] {
        const hints: string[] = [];
        const frequencyIds = new Set(['FREQ', 'FREQUENCY', 'TIME_FORMAT', 'TIMEFMT']);

        relationStats.forEach((valueMap, dimensionId) => {
            valueMap.forEach((relatedMap, valueCode) => {
                relatedMap.forEach((relatedValues, otherDimensionId) => {
                    if (!frequencyIds.has(otherDimensionId)) {
                        return;
                    }
                    if (relatedValues.size === 0) {
                        return;
                    }

                    const target = this.formatCode(dimensionId, valueCode, codeLookup);
                    const formattedRelated = Array.from(relatedValues).map((code) =>
                        this.formatCode(otherDimensionId, code, codeLookup)
                    );

                    if (relatedValues.size === 1) {
                        hints.push(
                            `${dimensionId}=${target} only appears with ${otherDimensionId}=${formattedRelated[0]}. Use that frequency when requesting this slice.`
                        );
                    } else if (relatedValues.size <= 4) {
                        hints.push(
                            `${dimensionId}=${target} supports ${otherDimensionId} values ${formattedRelated.join(', ')}.`
                        );
                    }
                });
            });
        });

        return hints;
    }

    private formatCode(dimensionId: string, code: string, codeLookup: Map<string, CodeLookupEntry>): string {
        const entry = codeLookup.get(dimensionId);
        const codeMeta = entry?.codes.get(code);
        if (codeMeta?.name && codeMeta.name !== code) {
            return `${code} (${codeMeta.name})`;
        }
        return code;
    }

    private buildValueSamples(
        frequencyMap: Map<string, number>,
        codeLookup: Map<string, CodeItem>
    ): { values: DimensionAvailabilityValue[]; limitHit: boolean } {
        const entries = Array.from(frequencyMap.entries());
        entries.sort((a, b) => {
            const diff = b[1] - a[1];
            if (diff !== 0) {
                return diff;
            }
            return a[0].localeCompare(b[0]);
        });

        const limited = entries.slice(0, SAMPLE_VALUE_LIMIT);
        const values: DimensionAvailabilityValue[] = limited.map(([code, count]) => {
            const codeMeta = codeLookup.get(code);
            return {
                code,
                label: codeMeta?.name || undefined,
                description: codeMeta?.description || undefined,
                seriesCount: count
            };
        });

        AGGREGATE_CODE_CANDIDATES.forEach((candidate) => {
            if (frequencyMap.has(candidate) && !values.some((entry) => entry.code === candidate)) {
                const codeMeta = codeLookup.get(candidate);
                values.push({
                    code: candidate,
                    label: codeMeta?.name || undefined,
                    description: codeMeta?.description || undefined,
                    seriesCount: frequencyMap.get(candidate) ?? 0
                });
            }
        });

        return { values, limitHit: entries.length > limited.length };
    }

    private buildMissingSamples(
        observedValues: Set<string>,
        codeLookup: Map<string, CodeItem>
    ): DimensionAvailabilityValue[] {
        const missing: DimensionAvailabilityValue[] = [];
        for (const [code, codeMeta] of codeLookup.entries()) {
            if (observedValues.has(code)) {
                continue;
            }
            missing.push({
                code,
                label: codeMeta?.name || undefined,
                description: codeMeta?.description || undefined
            });
            if (missing.length >= MISSING_VALUE_LIMIT) {
                break;
            }
        }
        return missing;
    }

    private buildDimensionNotes(
        dimensionId: string,
        observedCount: number,
        fixedValue: string | undefined,
        limitHit: boolean,
        missingCount: number,
        codeLookup: Map<string, CodeItem>
    ): string[] {
        const notes: string[] = [];
        if (fixedValue) {
            const label = codeLookup.get(fixedValue)?.name;
            notes.push(
                `Fixed dimension: always ${fixedValue}${label ? ` (${label})` : ''}.`
            );
        } else if (observedCount === 0) {
            notes.push('No observed values detected for this dimension in the latest sample.');
        } else if (observedCount === 1) {
            notes.push('Only one observed code is published for this dimension.');
        } else {
            notes.push('Supports multi-select arrays; keep the list short and prefer aggregates before drilling down.');
        }

        if (limitHit) {
            notes.push('Only a subset of codes is shown here. Refer to metadata.dimensions for the full list.');
        }
        if (missingCount > 0) {
            notes.push('Some metadata codes currently have no published observations; requesting them may return 404.');
        }
        if (this.hasAggregateCode(codeLookup)) {
            notes.push('Includes aggregate totals (e.g., TOT/ALL) for high-level requests.');
        }
        if (/REGION/i.test(dimensionId) && observedCount > SAMPLE_VALUE_LIMIT) {
            notes.push('For geography, combine REGION_TYPE and STATE to target manageable slices.');
        }
        return notes;
    }

    private hasAggregateCode(codeLookup: Map<string, CodeItem>): boolean {
        for (const candidate of AGGREGATE_CODE_CANDIDATES) {
            if (codeLookup.has(candidate)) {
                return true;
            }
        }
        return false;
    }

    private updateRelationStats(
        activeTargets: Set<string>,
        relationStats: RelationStats,
        coordinates: Record<string, DimensionValueSummary>,
        dimensionOrder: string[]
    ): void {
        activeTargets.forEach((dimensionId) => {
            const value = coordinates[dimensionId]?.code;
            if (!value) {
                return;
            }
            let dimensionRelations = relationStats.get(dimensionId);
            if (!dimensionRelations) {
                dimensionRelations = new Map();
                relationStats.set(dimensionId, dimensionRelations);
            }
            let valueRelations = dimensionRelations.get(value);
            if (!valueRelations) {
                valueRelations = new Map();
                dimensionRelations.set(value, valueRelations);
            }
            dimensionOrder.forEach((otherDimensionId) => {
                if (otherDimensionId === dimensionId) {
                    return;
                }
                const otherValue = coordinates[otherDimensionId]?.code;
                if (!otherValue) {
                    return;
                }
                const existing = valueRelations.get(otherDimensionId) ?? new Set<string>();
                existing.add(otherValue);
                valueRelations.set(otherDimensionId, existing);
            });
        });
    }

    private buildCachePath(datasetKey: string): string {
        const safeName = datasetKey.replace(/[^a-zA-Z0-9_-]/g, '_');
        return path.join(this.availabilityCacheDir, `${safeName}.json`);
    }

    private async ensureCacheDir(): Promise<void> {
        try {
            await fs.mkdir(this.availabilityCacheDir, { recursive: true });
        } catch (error) {
            logger.warn('Failed to create availability cache directory', {
                dir: this.availabilityCacheDir,
                error
            });
        }
    }

    private async readCache(cachePath: string): Promise<DatasetAvailabilityMap | null> {
        try {
            const serialized = await fs.readFile(cachePath, 'utf-8');
            const parsed = JSON.parse(serialized);
            if (parsed && typeof parsed === 'object') {
                return parsed as DatasetAvailabilityMap;
            }
        } catch (error) {
            if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') {
                logger.debug('Failed to read availability cache', { cachePath, error });
            }
        }
        return null;
    }

    private async writeCache(cachePath: string, payload: DatasetAvailabilityMap): Promise<void> {
        try {
            await fs.writeFile(cachePath, JSON.stringify(payload, null, 2), 'utf-8');
        } catch (error) {
            logger.warn('Failed to persist availability cache', { cachePath, error });
        }
    }
}
