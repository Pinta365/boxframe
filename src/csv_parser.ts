/**
 * CSV Parser using Deno Standard Library
 */

import { parse } from "@std/csv/parse";
import { DataFrame } from "./dataframe.ts";
import { Series } from "./series.ts";
import { readFile } from "@cross/fs";
import { CurrentRuntime, Runtime } from "@cross/runtime";
import type { DataValue, DType } from "./types.ts";
import { inferDType } from "./types.ts";

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
    /** Column data types - can be specified by column name or index */
    dtypes?: Record<string, DType> | DType[];
    /** Whether to infer data types automatically (default: true) */
    inferTypes?: boolean;
    /** Values to treat as null/NaN */
    naValues?: string[];
}

/**
 * Convert a string value to the specified data type
 */
function convertValue(value: string, dtype: DType, naValues: string[] = []): DataValue {
    // Check if value should be treated as null
    if (naValues.includes(value) || value === "" || value === "null" || value === "NULL") {
        return null;
    }

    switch (dtype) {
        case "int32": {
            const intVal = parseInt(value, 10);
            return isNaN(intVal) ? null : intVal;
        }

        case "float64": {
            const floatVal = parseFloat(value);
            return isNaN(floatVal) ? null : floatVal;
        }

        case "bool": {
            const lowerValue = value.toLowerCase();
            if (lowerValue === "true") {
                return true;
            }
            if (lowerValue === "false") {
                return false;
            }
            return null;
        }

        case "datetime": {
            const dateVal = new Date(value);
            return isNaN(dateVal.getTime()) ? null : dateVal;
        }

        case "string":
        default: {
            return value;
        }
    }
}

/**
 * Check if a string looks like a datetime
 */
function looksLikeDateTime(value: string): boolean {
    const datePatterns = [
        /^\d{4}-\d{2}-\d{2}$/, // YYYY-MM-DD
        /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/, // YYYY-MM-DD HH:MM:SS
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, // YYYY-MM-DDTHH:MM:SS
        /^\d{2}\/\d{2}\/\d{4}$/, // MM/DD/YYYY
        /^\d{2}-\d{2}-\d{4}$/, // MM-DD-YYYY
        /^\d{4}\/\d{2}\/\d{2}$/, // YYYY/MM/DD
        /^\d{1,2}\/\d{1,2}\/\d{4}$/, // M/D/YYYY or MM/DD/YYYY
        /^\d{1,2}-\d{1,2}-\d{4}$/, // M-D-YYYY or MM-DD-YYYY
    ];

    return datePatterns.some((pattern) => pattern.test(value.trim()));
}

/**
 * Check if a string can be parsed as a valid datetime
 */
function canParseAsDateTime(value: string): boolean {
    /* TODO: Im considering if its better to just try to convert to date to see if it can be parsed as a datetime instead of regexing it
        Basically comes down to performance vs accuracy*/
    if (!looksLikeDateTime(value)) {
        return false;
    }

    const date = new Date(value);
    return !isNaN(date.getTime());
}

/**
 * Infer data types for columns based on sample values
 */
function inferColumnTypes(
    data: (string | null)[][],
    columnNames: string[],
    sampleSize: number = 100,
): Record<string, DType> {
    const types: Record<string, DType> = {};

    for (let colIndex = 0; colIndex < columnNames.length; colIndex++) {
        const columnName = columnNames[colIndex];
        const sampleValues = data
            .slice(0, sampleSize)
            .map((row) => row[colIndex])
            .filter((val) => val !== null && val !== undefined && val !== "");

        if (sampleValues.length === 0) {
            types[columnName] = "string";
            continue;
        }

        const allLookLikeDates = sampleValues.every((val) => val !== null && looksLikeDateTime(val));
        const allCanParseAsDates = sampleValues.every((val) => val !== null && canParseAsDateTime(val));

        if (allLookLikeDates && allCanParseAsDates) {
            types[columnName] = "datetime";
            continue;
        }

        const convertedValues: DataValue[] = [];
        for (const val of sampleValues) {
            if (val) {
                const numVal = Number(val);
                if (!isNaN(numVal)) {
                    convertedValues.push(numVal);
                } else {
                    convertedValues.push(val);
                }
            }
        }

        const firstValue = convertedValues[0];
        if (firstValue !== undefined) {
            types[columnName] = inferDType(firstValue);
        } else {
            types[columnName] = "string";
        }
    }

    return types;
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
        dtypes,
        inferTypes = true,
        naValues = [],
    } = options;

    if (content.trim() === "") {
        throw new Error("Empty content is not allowed");
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
                const emptyData: Record<string, DataValue[]> = {};
                for (const header of headers) {
                    emptyData[header] = [];
                }
                return new DataFrame(emptyData);
            }
        }
        return new DataFrame({});
    }

    let columnNames: string[];
    if (hasHeader || columns) {
        const dataObjects = parsedData as Record<string, unknown>[];
        columnNames = columns || Object.keys(dataObjects[0]);
    } else {
        const dataArrays = parsedData as string[][];
        const numCols = dataArrays[0].length;
        columnNames = Array.from({ length: numCols }, (_, i) => `col_${i}`);
    }

    const stringData: (string | null)[][] = [];
    if (hasHeader || columns) {
        const dataObjects = parsedData as Record<string, unknown>[];
        for (const row of dataObjects) {
            const stringRow: (string | null)[] = [];
            for (const col of columnNames) {
                const value = row[col];
                stringRow.push(value !== null && value !== undefined ? String(value) : null);
            }
            stringData.push(stringRow);
        }
    } else {
        const dataArrays = parsedData as string[][];
        for (const row of dataArrays) {
            stringData.push([...row]);
        }
    }

    let columnTypes: Record<string, DType>;
    if (dtypes) {
        const inferredTypes = inferTypes ? inferColumnTypes(stringData, columnNames) : {};

        if (Array.isArray(dtypes)) {
            columnTypes = {};
            for (let i = 0; i < columnNames.length; i++) {
                const dtype = dtypes[i];
                if (dtype && ["int32", "float64", "string", "bool", "datetime"].includes(dtype)) {
                    columnTypes[columnNames[i]] = dtype;
                } else if (inferTypes && inferredTypes[columnNames[i]]) {
                    columnTypes[columnNames[i]] = inferredTypes[columnNames[i]];
                } else {
                    columnTypes[columnNames[i]] = "string";
                }
            }
        } else {
            columnTypes = {};
            for (const col of columnNames) {
                const dtype = dtypes[col];
                if (dtype && ["int32", "float64", "string", "bool", "datetime"].includes(dtype)) {
                    columnTypes[col] = dtype;
                } else if (inferTypes && inferredTypes[col]) {
                    columnTypes[col] = inferredTypes[col];
                } else {
                    columnTypes[col] = "string";
                }
            }
        }
    } else if (inferTypes) {
        columnTypes = inferColumnTypes(stringData, columnNames);
    } else {
        columnTypes = {};
        for (const col of columnNames) {
            columnTypes[col] = "string";
        }
    }

    const data: Record<string, DataValue[]> = {};
    const seriesMap = new Map<string, Series>();

    for (const col of columnNames) {
        data[col] = [];
    }

    for (const row of stringData) {
        for (let i = 0; i < columnNames.length; i++) {
            const col = columnNames[i];
            const value = row[i];
            const dtype = columnTypes[col];

            if (value === null || value === undefined) {
                data[col].push(null);
            } else {
                const convertedValue = convertValue(value, dtype, naValues);
                data[col].push(convertedValue);
            }
        }
    }

    for (const col of columnNames) {
        const series = new Series(data[col], {
            name: col,
            dtype: columnTypes[col],
        });
        seriesMap.set(col, series);
    }

    const df = new DataFrame(data);

    for (const [col, series] of seriesMap) {
        (df.data as Map<string, Series>).set(col, series);
    }

    return df;
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
