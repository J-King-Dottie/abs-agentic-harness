import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import logger from '../../utils/logger.js';
import { DataFlow, DataQueryOptions, DataStructureMetadata, ResolvedDataset } from '../../types/abs.js';

const execFileAsync = promisify(execFile);

export class RbaTablesCsvService {
    private readonly parserScriptPath: string;

    constructor(projectRoot: string) {
        this.parserScriptPath = path.join(projectRoot, 'scripts', 'rba_tables_csv.py');
    }

    supports(flow: DataFlow): boolean {
        return flow.flowType === 'rba_tables_csv';
    }

    async getMetadata(flow: DataFlow): Promise<DataStructureMetadata> {
        const csvPath = await this.downloadCsv(flow);
        try {
            return await this.runMetadata(flow, csvPath);
        } finally {
            await this.safeUnlink(csvPath);
        }
    }

    async query(flow: DataFlow, dataKey: string = 'all', options?: DataQueryOptions): Promise<ResolvedDataset> {
        const csvPath = await this.downloadCsv(flow);
        try {
            return await this.runResolve(flow, csvPath, dataKey, options);
        } finally {
            await this.safeUnlink(csvPath);
        }
    }

    async resolve(flow: DataFlow, options: DataQueryOptions & { dataKey?: string }): Promise<ResolvedDataset> {
        const dataKey = String(options.dataKey ?? '').trim() || 'all';
        return this.query(flow, dataKey, options);
    }

    private async runMetadata(flow: DataFlow, csvPath: string): Promise<DataStructureMetadata> {
        const { stdout } = await execFileAsync('python3', [
            this.parserScriptPath,
            'metadata',
            '--csv',
            csvPath,
            '--dataset-id',
            flow.id,
            '--agency-id',
            flow.agencyID,
            '--version',
            flow.version,
            '--name',
            flow.name,
            '--description',
            flow.description,
            '--curation-json',
            JSON.stringify(flow.curation ?? {}),
        ], {
            cwd: path.dirname(this.parserScriptPath),
            maxBuffer: 8 * 1024 * 1024,
        });

        return JSON.parse(stdout) as DataStructureMetadata;
    }

    private async runResolve(
        flow: DataFlow,
        csvPath: string,
        dataKey: string,
        options?: DataQueryOptions,
    ): Promise<ResolvedDataset> {
        const { stdout } = await execFileAsync('python3', [
            this.parserScriptPath,
            'resolve',
            '--csv',
            csvPath,
            '--dataset-id',
            flow.id,
            '--agency-id',
            flow.agencyID,
            '--version',
            flow.version,
            '--name',
            flow.name,
            '--description',
            flow.description,
            '--data-key',
            dataKey,
            '--detail',
            options?.detail ?? 'full',
            '--curation-json',
            JSON.stringify(flow.curation ?? {}),
        ], {
            cwd: path.dirname(this.parserScriptPath),
            maxBuffer: 32 * 1024 * 1024,
        });

        return JSON.parse(stdout) as ResolvedDataset;
    }

    private async downloadCsv(flow: DataFlow): Promise<string> {
        if (!flow.sourceUrl) {
            throw new Error(`Custom flow ${flow.id} is missing sourceUrl`);
        }

        const tmpPath = path.join(
            os.tmpdir(),
            `${flow.id.toLowerCase()}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}.csv`,
        );
        await execFileAsync('curl', [
            '-sSL',
            flow.sourceUrl,
            '-o',
            tmpPath,
        ], {
            cwd: path.dirname(this.parserScriptPath),
            maxBuffer: 1024 * 1024,
        });
        logger.info('Downloaded RBA statistical table CSV', {
            datasetId: flow.id,
            sourceUrl: flow.sourceUrl,
            tmpPath,
        });
        return tmpPath;
    }

    private async safeUnlink(filePath: string): Promise<void> {
        try {
            await fs.unlink(filePath);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                logger.warn('Failed to remove temporary RBA CSV', { filePath, error });
            }
        }
    }
}
