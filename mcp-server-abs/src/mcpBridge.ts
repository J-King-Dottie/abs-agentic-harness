#!/usr/bin/env node

import path from "node:path";
import { fileURLToPath } from "node:url";

import { DataFlowService } from "./services/abs/DataFlowService.js";
import { DatasetResolver } from "./services/abs/DatasetResolver.js";
import { DatasetAvailabilityService } from "./services/abs/DatasetAvailabilityService.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const projectRoot = path.resolve(__dirname, "..");
const cacheDir = path.join(projectRoot, "cache");
const cachePath = path.join(cacheDir, "dataflows.full.json");
const availabilityCacheDir = path.join(cacheDir, "availability");

async function ensureCacheDir() {
  await import("node:fs/promises").then(({ mkdir }) =>
    mkdir(cacheDir, { recursive: true })
  );
}

const COMPACT_JSON = process.env.MCP_BRIDGE_COMPACT === "1";

function emit(result: unknown) {
  const payload = COMPACT_JSON
    ? JSON.stringify(result)
    : JSON.stringify(result, null, 2);
  process.stdout.write(payload);
}

async function main() {
  await ensureCacheDir();

  const [command, rawPayload] = process.argv.slice(2);
  if (!command) {
    throw new Error("A command is required (list-dataflows | resolve-dataset).");
  }

  let payload: Record<string, unknown> = {};
  if (rawPayload) {
    try {
      payload = JSON.parse(rawPayload);
    } catch (error) {
      throw new Error(`Payload must be valid JSON. Received: ${rawPayload}`);
    }
  }

  const dataFlowService = new DataFlowService(cachePath);
  const resolver = new DatasetResolver(dataFlowService);
  const availabilityService = new DatasetAvailabilityService(
    dataFlowService,
    availabilityCacheDir
  );

  if (command === "list-dataflows") {
    const forceRefresh = Boolean(payload.forceRefresh);
    const flows = await dataFlowService.getDataFlows(forceRefresh);
    const response = {
      total: flows.length,
      dataflows: flows,
    };
    emit(response);
    return;
  }

  if (command === "get-dataflow-metadata") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("get-dataflow-metadata requires a datasetId string.");
    }
    const metadata = await dataFlowService.getDataStructureForDataflow(
      datasetId,
      Boolean(payload.forceRefresh)
    );
    emit(metadata);
    return;
  }

  if (command === "query-dataset") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("query-dataset requires a datasetId string.");
    }
    const allowedDetails = new Set([
      "full",
      "dataonly",
      "serieskeysonly",
      "nodata",
    ]);
    const detailValue =
      typeof payload.detail === "string" && allowedDetails.has(payload.detail)
        ? (payload.detail as "full" | "dataonly" | "serieskeysonly" | "nodata")
        : undefined;
    const data = await dataFlowService.getFlowData(
      datasetId,
      typeof payload.dataKey === "string" ? payload.dataKey : "all",
      {
        startPeriod:
          typeof payload.startPeriod === "string" ? payload.startPeriod : undefined,
        endPeriod:
          typeof payload.endPeriod === "string" ? payload.endPeriod : undefined,
        detail: detailValue,
        dimensionAtObservation:
          typeof payload.dimensionAtObservation === "string"
            ? payload.dimensionAtObservation
            : undefined,
      }
    );
    emit(data);
    return;
  }

  if (command === "resolve-dataset") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("resolve-dataset requires a datasetId string.");
    }

    const allowedDetails = new Set([
      "full",
      "dataonly",
      "serieskeysonly",
      "nodata",
    ]);

    const detailValue =
      typeof payload.detail === "string" && allowedDetails.has(payload.detail)
        ? (payload.detail as "full" | "dataonly" | "serieskeysonly" | "nodata")
        : undefined;

    const result = await resolver.resolve({
      datasetId,
      dataKey: typeof payload.dataKey === "string" ? payload.dataKey : undefined,
      startPeriod:
        typeof payload.startPeriod === "string" ? payload.startPeriod : undefined,
      endPeriod:
        typeof payload.endPeriod === "string" ? payload.endPeriod : undefined,
      detail: detailValue,
      dimensionAtObservation:
        typeof payload.dimensionAtObservation === "string"
          ? payload.dimensionAtObservation
          : undefined,
      forceRefresh: Boolean(payload.forceRefresh),
    });

    emit(result);
    return;
  }

  if (command === "describe-availability") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("describe-availability requires a datasetId string.");
    }
    const result = await availabilityService.getAvailabilityMap(datasetId, {
      forceRefresh: Boolean(payload.forceRefresh),
    });
    emit(result);
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  const message =
    error instanceof Error ? error.message : `Unexpected error: ${String(error)}`;
  console.error(message);
  process.exit(1);
});
