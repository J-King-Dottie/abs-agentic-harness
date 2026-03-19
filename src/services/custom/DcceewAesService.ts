import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import logger from '../../utils/logger.js';
import { DataFlow, DataQueryOptions, DataStructureMetadata, ResolvedDataset } from '../../types/abs.js';

const execFileAsync = promisify(execFile);
const DOWNLOAD_TIMEOUT_MS = 45000;
const PARSE_TIMEOUT_MS = 120000;
const PYTHON_EXECUTABLES = process.platform === 'win32'
    ? [['python'], ['py', '-3'], ['python3']]
    : [['python3'], ['python']];
const PYTHON_DOWNLOAD_SCRIPT = `
import sys
import time
from pathlib import Path
import requests

url = sys.argv[1]
out_path = Path(sys.argv[2])
for attempt in range(1, 4):
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=(20, 60))
        response.raise_for_status()
        payload = response.content
        if len(payload) < 4 or payload[:2] != b'PK':
            raise ValueError('Downloaded payload is not a valid XLSX file')
        out_path.write_bytes(payload)
        break
    except Exception as exc:
        last_error = exc
        if attempt == 3:
            raise
        time.sleep(attempt)
`;

interface CustomSheetGroup {
    id: string;
    description: string;
    sheets: string[];
}

interface CustomCurationConfig {
    ignoredSheets?: string[];
    sheetGroups?: CustomSheetGroup[];
}

export class DcceewAesService {
    private readonly parserScriptPath: string;

    constructor(projectRoot: string) {
        this.parserScriptPath = path.join(projectRoot, 'scripts', 'dcceew_aes_xlsx.py');
    }

    supports(flow: DataFlow): boolean {
        return flow.flowType === 'dcceew_aes_xlsx';
    }

    async getMetadata(flow: DataFlow): Promise<DataStructureMetadata> {
        const workbookPath = await this.downloadWorkbook(flow);
        try {
            return await this.runMetadata(flow, workbookPath);
        } finally {
            await this.safeUnlink(workbookPath);
        }
    }

    async query(flow: DataFlow, dataKey: string = 'all', options?: DataQueryOptions): Promise<ResolvedDataset> {
        const workbookPath = await this.downloadWorkbook(flow);
        try {
            return await this.runResolve(flow, workbookPath, dataKey, options);
        } finally {
            await this.safeUnlink(workbookPath);
        }
    }

    async resolve(flow: DataFlow, options: DataQueryOptions & { dataKey?: string }): Promise<ResolvedDataset> {
        const dataKey = String(options.dataKey ?? '').trim() || 'all';
        return this.query(flow, dataKey, options);
    }

    private async runMetadata(flow: DataFlow, workbookPath: string): Promise<DataStructureMetadata> {
        const { stdout } = await this.execPython([
            this.parserScriptPath,
            'metadata',
            '--xlsx',
            workbookPath,
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
        ], PARSE_TIMEOUT_MS, 4 * 1024 * 1024);

        return JSON.parse(stdout) as DataStructureMetadata;
    }

    private async runResolve(
        flow: DataFlow,
        workbookPath: string,
        dataKey: string,
        options?: DataQueryOptions,
    ): Promise<ResolvedDataset> {
        const { stdout } = await this.execPython([
            this.parserScriptPath,
            'resolve',
            '--xlsx',
            workbookPath,
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
        ], PARSE_TIMEOUT_MS, 16 * 1024 * 1024);

        return JSON.parse(stdout) as ResolvedDataset;
    }

    private async downloadWorkbook(flow: DataFlow): Promise<string> {
        if (!flow.sourceUrl) {
            throw new Error(`Custom flow ${flow.id} is missing sourceUrl`);
        }

        const tmpPath = path.join(
            os.tmpdir(),
            `${flow.id.toLowerCase()}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}.xlsx`,
        );
        logger.info('Starting DCCEEW AES workbook download', {
            datasetId: flow.id,
            sourceUrl: flow.sourceUrl,
            downloadMethod: 'python_requests_runtime_fetch',
        });
        try {
            const result = await this.execPython([
                '-c',
                PYTHON_DOWNLOAD_SCRIPT,
                flow.sourceUrl,
                tmpPath,
            ], DOWNLOAD_TIMEOUT_MS + 10000, 1024 * 1024);
            const stats = await fs.stat(tmpPath);
            logger.info('Downloaded DCCEEW AES workbook', {
                datasetId: flow.id,
                sourceUrl: flow.sourceUrl,
                tmpPath,
                bytes: stats.size,
                downloadMethod: 'python_requests_runtime_fetch',
                pythonCommand: result.pythonCommand,
            });
            return tmpPath;
        } catch (error) {
            await this.safeUnlink(tmpPath);
            logger.error('DCCEEW AES workbook download failed', {
                datasetId: flow.id,
                sourceUrl: flow.sourceUrl,
                downloadMethod: 'python_requests_runtime_fetch',
                error: this.serializeExecError(error),
            });
            throw new Error(
                `Failed to download live DCCEEW workbook for ${flow.id} from ${flow.sourceUrl}: ${this.formatExecError(error)}`,
            );
        }
    }

    private async safeUnlink(filePath: string): Promise<void> {
        try {
            await fs.unlink(filePath);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                logger.warn('Failed to remove temporary workbook', { filePath, error });
            }
        }
    }

    private async execPython(
        args: string[],
        timeout: number,
        maxBuffer: number,
    ): Promise<{ stdout: string; stderr: string; pythonCommand: string[] }> {
        let lastError: unknown;
        for (const command of PYTHON_EXECUTABLES) {
            try {
                const result = await execFileAsync(command[0], [...command.slice(1), ...args], {
                    cwd: path.dirname(this.parserScriptPath),
                    maxBuffer,
                    timeout,
                });
                return {
                    stdout: result.stdout,
                    stderr: result.stderr,
                    pythonCommand: command,
                };
            } catch (error) {
                lastError = error;
                logger.warn('Python command failed for DCCEEW adapter', {
                    pythonCommand: command,
                    error: this.serializeExecError(error),
                });
            }
        }
        throw lastError instanceof Error ? lastError : new Error(String(lastError));
    }

    private serializeExecError(error: unknown): Record<string, unknown> {
        if (!error || typeof error !== 'object') {
            return { message: String(error) };
        }
        const execError = error as NodeJS.ErrnoException & {
            stdout?: string;
            stderr?: string;
            code?: string | number;
            signal?: string;
            killed?: boolean;
            cmd?: string;
        };
        return {
            message: execError.message,
            code: execError.code,
            signal: execError.signal,
            killed: execError.killed,
            cmd: execError.cmd,
            stdout: String(execError.stdout ?? '').slice(0, 1000),
            stderr: String(execError.stderr ?? '').slice(0, 1000),
        };
    }

    private formatExecError(error: unknown): string {
        const details = this.serializeExecError(error);
        return JSON.stringify(details);
    }

}
