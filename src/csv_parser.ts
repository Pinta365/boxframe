/**
 * CSV Parser using Deno Standard Library
 */

import { parse } from "@std/csv/parse";
import { DataFrame } from "./dataframe.ts";
import { readFile } from "@cross/fs/io";
import { CurrentRuntime, Runtime } from "@cross/runtime";

/**
 * CSV parsing options
 */
export interface CsvParseOptions {
    /** Whether the first row contains headers */
    hasHeader?: boolean;
    /** Custom delimiter (default: ',') */
    delimiter?: string;
    /** Number of rows to skip from the beginning */
    skipRows?: number;
    /** Custom column names (used when hasHeader is false) */
    columns?: string[];
    /** Comment character to ignore lines starting with this character */
    comment?: string;
}

/**
 * Parse CSV content using Deno standard library
 */
export function parseCsv(content: string, options: CsvParseOptions = {}): DataFrame {
    const {
        hasHeader = true,
        delimiter = ",",
        skipRows = 0,
        columns,
        comment,
    } = options;

    if (content.trim() === "") {
        throw new Error("Empty path is not allowed");
    }

    let processedContent = content;
    if (skipRows > 0) {
        const lines = content.split("\n");
        processedContent = lines.slice(skipRows).join("\n");
    }

    const parseOptions: {
        skipFirstRow: boolean;
        separator: string;
        comment?: string;
        columns?: string[];
    } = {
        skipFirstRow: hasHeader,
        separator: delimiter,
    };

    if (comment) {
        parseOptions.comment = comment;
    }

    if (columns && !hasHeader) {
        parseOptions.columns = columns;
    }

    const parsedData = parse(processedContent, parseOptions);

    if (parsedData.length === 0) {
        if (hasHeader && processedContent.trim().length > 0) {
            const lines = processedContent.trim().split("\n");
            if (lines.length === 1) {
                const headers = lines[0].split(delimiter || ",").map((h) => h.trim());
                const emptyData: Record<string, unknown[]> = {};
                for (const header of headers) {
                    emptyData[header] = [];
                }
                return new DataFrame(emptyData);
            }
        }
        return new DataFrame({});
    }

    if (hasHeader || columns) {
        const dataObjects = parsedData as Record<string, unknown>[];

        if (dataObjects.length === 0) {
            return new DataFrame({});
        }

        const columnNames = columns || Object.keys(dataObjects[0]);

        const data: Record<string, unknown[]> = {};
        for (const col of columnNames) {
            data[col] = [];
        }

        for (const row of dataObjects) {
            for (const col of columnNames) {
                const value = row[col];

                if (value !== null && value !== undefined) {
                    data[col].push(value);
                } else {
                    data[col].push(null);
                }
            }
        }

        return new DataFrame(data);
    } else {
        const dataArrays = parsedData as string[][];

        if (dataArrays.length === 0) {
            return new DataFrame({});
        }

        const numCols = dataArrays[0].length;
        const columnNames = Array.from({ length: numCols }, (_, i) => `col_${i}`);

        const data: Record<string, unknown[]> = {};
        for (const col of columnNames) {
            data[col] = [];
        }

        for (const row of dataArrays) {
            for (let i = 0; i < columnNames.length; i++) {
                const col = columnNames[i];
                const value = row[i];

                if (value !== null && value !== undefined && value !== "") {
                    const numValue = Number(value);
                    if (!isNaN(numValue)) {
                        data[col].push(numValue);
                    } else {
                        data[col].push(value);
                    }
                } else {
                    data[col].push(null);
                }
            }
        }

        return new DataFrame(data);
    }
}

/**
 * Parse CSV from file path or URL
 */
export async function parseCsvFromFile(filePath: string, options: CsvParseOptions = {}): Promise<DataFrame> {
    let content: string;

    if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
        try {
            const response = await fetch(filePath);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            content = await response.text();
        } catch (error) {
            throw new Error(`Failed to fetch CSV from URL: ${error instanceof Error ? error.message : String(error)}`);
        }
    } else {
        // Check if we're in a browser environment for local file access
        if (CurrentRuntime === Runtime.Browser) {
            throw new Error(
                "Local file reading is not supported in browser environments. Use BoxFrame.parseCsv() with string content or provide a URL instead.",
            );
        }

        try {
            content = await readFile(filePath, "utf-8");
        } catch (error) {
            throw new Error(`Failed to read CSV file: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    return parseCsv(content, options);
}

/**
 * Parse CSV with streaming support for large files
 */
export async function parseCsvStream(filePath: string, options: CsvParseOptions = {}): Promise<DataFrame> {
    // For now, we'll use the regular file reading approach
    // In the future, we could implement streaming with CsvParseStream
    return await parseCsvFromFile(filePath, options);
}
