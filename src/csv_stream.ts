import { DataFrame } from "./dataframe.ts";
import { parseCsv } from "./csv_parser.ts";
import type { CsvParseOptions } from "./csv_parser.ts";
import { CurrentRuntime, Runtime } from "@cross/runtime";
import { open, stat } from "@cross/fs";

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

export interface ParseCsvStreamOptions extends CsvParseOptions {
    /** Number of lines per batch to parse (default: 25,000) */
    batchSize?: number;
    /** Callback invoked for each parsed batch DataFrame */
    onBatch: (df: DataFrame) => void | Promise<void>;
    /** Optional progress callback reporting bytes read and rows processed */
    onProgress?: (progress: { bytesRead: number; rowsProcessed: number }) => void;
}

/**
 * Parse a CSV file in batches using a streaming reader.
 *
 * Reads the file progressively, parsing batches of lines and invoking the
 * provided onBatch callback with a DataFrame for each batch.
 */
export async function parseCsvBatchedStream(filePath: string, options: ParseCsvStreamOptions): Promise<void> {
    if (CurrentRuntime === Runtime.Browser) {
        throw new Error("parseCsvStream only supports file paths outside browser environments.");
    }

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
 */
export async function parseCsvStream(
    filePath: string,
    options: Omit<ParseCsvStreamOptions, "onBatch"> = {},
): Promise<DataFrame> {
    const accum: Record<string, unknown[]> = {};
    let columns: string[] = [];

    const mergedOptions = { batchSize: 25000, inferTypes: true, ...options } as Omit<ParseCsvStreamOptions, "onBatch">;

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
