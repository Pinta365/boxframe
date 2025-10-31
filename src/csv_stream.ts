import { DataFrame } from "./dataframe.ts";
import { Series } from "./series.ts";
import { parseCsv } from "./csv_parser.ts";
import type { CsvParseOptions } from "./csv_parser.ts";
import { CurrentRuntime, Runtime } from "@cross/runtime";
import { open, stat } from "@cross/fs";
import type { DType } from "./types.ts";
import { WorkerPool } from "./runtime/worker_pool.ts";
import { getDefaultWorkerModuleUrl } from "./runtime/create_worker.ts";

export interface AnalyzeCsvOptions {
    /** Number of lines to sample from the start of the file (default: 1000) */
    sampleLines?: number;
    /** Parse options forwarded to parseCsv for the sampled content */
    parseOptions?: CsvParseOptions;
}

export interface AnalyzeCsvResult {
    /** Column names detected in the sample */
    columns: string[];
    /** Inferred data types by column name */
    dtypes: Record<string, string>;
    /** Rough estimate of total rows based on sample average line length and file size */
    estimatedRows: number;
    /** Actual number of rows parsed from the sample */
    sampleRows: number;
}

/**
 * Quickly analyze a CSV file using a small sampled prefix.
 *
 * Provides detected columns, inferred dtypes, a rough estimated row count,
 * and the number of rows in the parsed sample.
 */
export async function analyzeCsv(filePath: string, options: AnalyzeCsvOptions = {}): Promise<AnalyzeCsvResult> {
    if (CurrentRuntime === Runtime.Browser) {
        throw new Error("analyzeCsv only supports file paths/URLs outside browser environments.");
    }

    const sampleLines = options.sampleLines ?? 1000;
    const content = await readFirstNLines(filePath, sampleLines);

    const df = parseCsv(content, options.parseOptions);

    const dtypes: Record<string, string> = {};
    for (const col of df.columns) {
        const series = (df as unknown as { data: Map<string, { dtype: string }> }).data.get(col);
        dtypes[col] = series?.dtype ?? "string";
    }

    const avgLineLength = content.length / Math.max(df.shape[0], 1);
    const fileInfo = await stat(filePath);
    const estimatedRows = Math.floor(fileInfo.size / Math.max(avgLineLength, 1));

    return {
        columns: df.columns,
        dtypes,
        estimatedRows,
        sampleRows: df.shape[0],
    };
}

/**
 * Worker-enhanced options for CSV streaming
 */
export interface WorkerEnhancedOptions {
    /** Enable worker-based parsing (default: false) */
    useWorkers?: boolean;
    /** Number of workers to use (default: Math.max(1, cores - 1)) */
    workerCount?: number;
    /** Preserve order of batches (default: true for parseCsvStream, false for batched) */
    preserveOrder?: boolean;
    /** Pre-computed schema (columns and dtypes) to skip inference */
    schema?: { columns: string[]; dtypes: Record<string, DType> };
    /** Abort signal for cancellation */
    abortSignal?: AbortSignal;
    /** Custom worker module URL (advanced) */
    workerModule?: string | URL;
}

export interface ParseCsvStreamOptions extends CsvParseOptions, WorkerEnhancedOptions {
    /** Number of lines per batch to parse (default: 25,000) */
    batchSize?: number;
    /** Callback invoked for each parsed batch DataFrame */
    onBatch: (df: DataFrame) => void | Promise<void>;
    /** Optional progress callback reporting bytes read and rows processed */
    onProgress?: (progress: { bytesRead: number; rowsProcessed: number }) => void;
}

/**
 * Infer schema from a sample of the CSV file
 */
async function inferSchemaFromSample(
    filePath: string,
    sampleLines: number,
    parseOptions: CsvParseOptions,
): Promise<{ columns: string[]; dtypes: Record<string, DType> }> {
    const content = await readFirstNLines(filePath, sampleLines);
    const df = parseCsv(content, parseOptions);

    const dtypes: Record<string, DType> = {};
    for (const col of df.columns) {
        const series = (df as unknown as { data: Map<string, { dtype: string }> }).data.get(col);
        dtypes[col] = (series?.dtype ?? "string") as DType;
    }

    return { columns: df.columns, dtypes };
}

/**
 * Get the number of CPU cores (with fallback)
 */
function getCpuCount(): number {
    if (typeof navigator !== "undefined" && navigator.hardwareConcurrency) {
        return navigator.hardwareConcurrency;
    }
    return 4;
}

/**
 * Parse a CSV file in batches using a streaming reader.
 *
 * Reads the file progressively, parsing batches of lines and invoking the
 * provided onBatch callback with a DataFrame for each batch.
 *
 * Supports optional worker-based parallel parsing.
 */
export async function parseCsvBatchedStream(filePath: string, options: ParseCsvStreamOptions): Promise<void> {
    if (CurrentRuntime === Runtime.Browser) {
        throw new Error("parseCsvStream only supports file paths outside browser environments.");
    }

    const useWorkers = options.useWorkers ?? false;
    const preserveOrder = options.preserveOrder ?? false;
    const batchSize = options.batchSize ?? 25000;

    if (!useWorkers) {
        return parseCsvBatchedStreamSingleThreaded(filePath, options);
    }

    const workerCount = options.workerCount ?? Math.max(1, getCpuCount() - 1);
    const workerModule = options.workerModule ?? getDefaultWorkerModuleUrl();
    const abortSignal = options.abortSignal;

    if (abortSignal?.aborted) {
        throw new Error("Operation aborted");
    }

    let schema = options.schema;
    if (!schema && (options.inferTypes ?? true)) {
        schema = await inferSchemaFromSample(filePath, 1000, options);
    }

    const pool = new WorkerPool({
        size: workerCount,
        moduleUrl: workerModule,
    });

    pool.onError = (error, seq) => {
        console.error("Worker error:", error, "seq:", seq);
        pool.close().catch(() => {});
        throw error instanceof Error ? error : new Error(String(error));
    };

    try {
        await pool.init();
        await pool.broadcast({
            type: "INIT",
            payload: {
                schema,
                delimiter: options.delimiter,
                naValues: options.naValues,
                hasHeader: options.hasHeader ?? true,
                parseOptions: {
                    comment: options.comment,
                    skipRows: options.skipRows,
                },
            },
        });

        const fh = await open(filePath, "r");
        const stream = fh.createReadStream();
        const decoder = new TextDecoder();

        let headerLine = "";
        let buffer = "";
        let batchLines: string[] = [];
        let nextSeq = 0;
        let lastSeqSent = -1;
        let batchesCompleted = 0;
        let bytesReadTotal = 0;
        let rowsProcessed = 0;
        const pendingResults = new Map<
            number,
            { columns: string[]; dtypes?: Record<string, DType>; data: Record<string, unknown[]>; rowsProcessed: number }
        >();

        const flushInOrder = async () => {
            while (pendingResults.has(nextSeq)) {
                const result = pendingResults.get(nextSeq)!;
                pendingResults.delete(nextSeq);

                let df: DataFrame;
                if (result.dtypes) {
                    const typedData: Record<string, unknown[]> = {};
                    for (const [col, values] of Object.entries(result.data)) {
                        typedData[col] = values;
                    }
                    df = new DataFrame(typedData);
                    for (const [col, dtype] of Object.entries(result.dtypes)) {
                        const series = df.data.get(col);
                        if (series) {
                            const seriesData = series.values;
                            df.data.set(col, new Series(seriesData, { name: col, dtype }));
                        }
                    }
                } else {
                    df = new DataFrame(result.data);
                }

                await options.onBatch(df);
                rowsProcessed += result.rowsProcessed;
                batchesCompleted++;
                if (options.onProgress) {
                    options.onProgress({ bytesRead: bytesReadTotal, rowsProcessed });
                }
                nextSeq++;
            }
        };

        pool.onResult = async (result) => {
            const { seq, payload } = result;
            const resultData = payload as {
                columns: string[];
                dtypes?: Record<string, DType>;
                data: Record<string, unknown[]>;
                rowsProcessed: number;
            };

            if (preserveOrder) {
                pendingResults.set(seq, resultData);
                await flushInOrder();
            } else {
                let df: DataFrame;
                if (resultData.dtypes) {
                    const typedData: Record<string, unknown[]> = {};
                    for (const [col, values] of Object.entries(resultData.data)) {
                        typedData[col] = values;
                    }
                    df = new DataFrame(typedData);
                    for (const [col, dtype] of Object.entries(resultData.dtypes)) {
                        const series = df.data.get(col);
                        if (series) {
                            const seriesData = series.values;
                            df.data.set(col, new Series(seriesData, { name: col, dtype }));
                        }
                    }
                } else {
                    df = new DataFrame(resultData.data);
                }
                await options.onBatch(df);
                rowsProcessed += resultData.rowsProcessed;
                batchesCompleted++;
                if (options.onProgress) {
                    options.onProgress({ bytesRead: bytesReadTotal, rowsProcessed });
                }
            }
        };

        try {
            for await (const chunk of stream as AsyncIterable<Uint8Array>) {
                if (abortSignal?.aborted) {
                    throw new Error("Operation aborted");
                }

                const text = decoder.decode(chunk, { stream: true });
                bytesReadTotal += text.length;
                buffer += text;

                let idx: number;
                while ((idx = buffer.indexOf("\n")) !== -1) {
                    const line = buffer.slice(0, idx);
                    buffer = buffer.slice(idx + 1);

                    if (!headerLine) {
                        headerLine = line;
                        continue;
                    }

                    if (line.trim().length === 0) continue;
                    batchLines.push(line);

                    if (batchLines.length >= batchSize) {
                        await pool.waitForCapacity();

                        if (abortSignal?.aborted) {
                            throw new Error("Operation aborted");
                        }

                        const seq = lastSeqSent + 1;
                        lastSeqSent = seq;
                        const batchText = batchLines.join("\n");
                        const batchBuffer = new TextEncoder().encode(batchText);

                        await pool.post({
                            seq,
                            payload: {
                                type: "BATCH",
                                payload: {
                                    seq,
                                    header: headerLine,
                                    linesBuffer: batchBuffer,
                                },
                            },
                            transfer: [batchBuffer.buffer],
                        });

                        batchLines = [];
                    }
                }
            }

            if (buffer.length > 0) {
                if (!headerLine) {
                    headerLine = buffer.trim();
                    buffer = "";
                } else {
                    const trimmed = buffer.trim();
                    if (trimmed.length > 0) {
                        batchLines.push(trimmed);
                    }
                    buffer = "";
                }
            }

            if (batchLines.length > 0) {
                await pool.waitForCapacity();
                const seq = lastSeqSent + 1;
                lastSeqSent = seq;
                const batchText = batchLines.join("\n");
                const batchBuffer = new TextEncoder().encode(batchText);

                await pool.post({
                    seq,
                    payload: {
                        type: "BATCH",
                        payload: {
                            seq,
                            header: headerLine,
                            linesBuffer: batchBuffer,
                        },
                    },
                    transfer: [batchBuffer.buffer],
                });
            }

            const expectedBatches = lastSeqSent + 1;

            if (expectedBatches > 0) {
                while (batchesCompleted < expectedBatches || pendingResults.size > 0) {
                    if (abortSignal?.aborted) {
                        throw new Error("Operation aborted");
                    }

                    await new Promise((resolve) => setTimeout(resolve, 10));
                    if (preserveOrder) {
                        await flushInOrder();
                    }
                }
            }
        } finally {
            try {
                await fh.close();
            } catch {
                // resource may already be closed
            }
        }
    } finally {
        await pool.close();
    }
}

/**
 * Single-threaded CSV batch streaming
 */
async function parseCsvBatchedStreamSingleThreaded(
    filePath: string,
    options: ParseCsvStreamOptions,
): Promise<void> {
    const batchSize = options.batchSize ?? 25000;
    const fh = await open(filePath, "r");
    const stream = fh.createReadStream();
    const decoder = new TextDecoder();

    let headerLine = "";
    let buffer = "";
    let batchLines: string[] = [];
    let bytesReadTotal = 0;
    let rowsProcessed = 0;

    try {
        for await (const chunk of stream as AsyncIterable<Uint8Array>) {
            const text = decoder.decode(chunk, { stream: true });
            bytesReadTotal += text.length;
            buffer += text;

            let idx: number;
            while ((idx = buffer.indexOf("\n")) !== -1) {
                const line = buffer.slice(0, idx);
                buffer = buffer.slice(idx + 1);

                if (!headerLine) {
                    headerLine = line;
                    continue;
                }

                if (line.trim().length === 0) continue;
                batchLines.push(line);

                if (batchLines.length >= batchSize) {
                    const content = headerLine + "\n" + batchLines.join("\n");
                    const df = parseCsv(content, { ...options, hasHeader: true });
                    await options.onBatch(df);
                    rowsProcessed += df.shape[0];
                    batchLines = [];
                    if (options.onProgress) options.onProgress({ bytesRead: bytesReadTotal, rowsProcessed });
                }
            }
        }

        if (buffer.length > 0) {
            if (!headerLine) {
                headerLine = buffer.trim();
                buffer = "";
            } else {
                const trimmed = buffer.trim();
                if (trimmed.length > 0) {
                    batchLines.push(trimmed);
                }
                buffer = "";
            }
        }

        if (batchLines.length > 0) {
            const content = headerLine + "\n" + batchLines.join("\n");
            const df = parseCsv(content, { ...options, hasHeader: true });
            await options.onBatch(df);
            rowsProcessed += df.shape[0];
            batchLines = [];
            if (options.onProgress) options.onProgress({ bytesRead: bytesReadTotal, rowsProcessed });
        }
    } finally {
        try {
            await fh.close();
        } catch {
            // resource may already be closed by the reader
        }
    }
}

/**
 * Parse a CSV file using streaming and return a single accumulated DataFrame.
 *
 * Internally uses the batched streaming parser and concatenates results
 * across batches to build one DataFrame.
 *
 * Supports optional worker-based parallel parsing
 */
export async function parseCsvStream(
    filePath: string,
    options: Omit<ParseCsvStreamOptions, "onBatch"> = {},
): Promise<DataFrame> {
    const accum: Record<string, unknown[]> = {};
    let columns: string[] = [];

    const mergedOptions = {
        batchSize: 25000,
        inferTypes: true,
        preserveOrder: options.preserveOrder ?? true,
        ...options,
    } as Omit<ParseCsvStreamOptions, "onBatch">;

    await parseCsvBatchedStream(filePath, {
        ...mergedOptions,
        onBatch: (df) => {
            if (columns.length === 0) {
                columns = df.columns;
                for (const col of columns) accum[col] = [];
            }
            for (const col of columns) {
                const series = (df as unknown as { data: Map<string, { values: unknown[] }> }).data.get(col);
                if (series) (accum[col] as unknown[]).push(...series.values);
            }
        },
    } as ParseCsvStreamOptions);

    return new DataFrame(accum);
}

/**
 * Read and return the first N lines from a file using a streaming reader.
 */
async function readFirstNLines(filePath: string, maxLines: number): Promise<string> {
    const fh = await open(filePath, "r");
    const stream = fh.createReadStream();
    const decoder = new TextDecoder();
    let content = "";
    let lineCount = 0;
    try {
        for await (const chunk of stream as AsyncIterable<Uint8Array>) {
            const chunkText = decoder.decode(chunk, { stream: true });
            let start = 0;
            while (true) {
                const nl = chunkText.indexOf("\n", start);
                if (nl === -1) {
                    content += chunkText.slice(start);
                    break;
                }
                content += chunkText.slice(start, nl + 1);
                lineCount++;
                if (lineCount >= maxLines) return content;
                start = nl + 1;
            }
        }
    } finally {
        try {
            await fh.close();
        } catch {
            // resource may already be closed
        }
    }
    return content;
}
