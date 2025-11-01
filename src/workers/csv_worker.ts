/// <reference lib="webworker" />
/**
 * CSV Worker Module
 * Handles CSV parsing in a worker context
 */

import { setupWorker } from "@cross/workers";
import { parse } from "@std/csv/parse";
import type { DataValue, DType } from "../types.ts";
import type { CsvParseOptions } from "../csv_parser.ts";

/**
 * Schema for parsing CSV batches
 */
interface Schema {
    columns: string[];
    dtypes: Record<string, DType>;
}

interface InitPayload {
    schema?: Schema;
    delimiter?: string;
    naValues?: string[];
    hasHeader?: boolean;
    parseOptions?: Partial<CsvParseOptions>;
}

interface BatchPayload {
    seq: number;
    header?: string;
    linesBuffer: Uint8Array;
}

/**
 * Convert a string value to the specified data type
 */
function convertValue(value: string, dtype: DType, naValues: string[] = []): DataValue {
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
 * Global state
 */
let schema: Schema | undefined;
let parseOptions: Partial<CsvParseOptions> = {};

/**
 * Parse CSV content and return serializable data
 */
function parseCsvBatch(
    content: string,
    schema?: Schema,
    parseOptions: Partial<CsvParseOptions> = {},
): { columns: string[]; dtypes?: Record<string, DType>; data: Record<string, DataValue[]>; rowsProcessed: number } {
    const {
        hasHeader = true,
        delimiter = ",",
        comment,
        naValues = [],
    } = parseOptions;

    if (content.trim() === "") {
        return { columns: [], data: {}, rowsProcessed: 0 };
    }

    const csvOptions: {
        skipFirstRow: boolean;
        separator: string;
        comment?: string;
        columns?: string[];
    } = {
        skipFirstRow: hasHeader,
        separator: delimiter,
    };

    if (comment) {
        csvOptions.comment = comment;
    }

    if (schema && !hasHeader) {
        csvOptions.columns = schema.columns;
    }

    const parsedData = parse(content, csvOptions);

    if (parsedData.length === 0) {
        const columns = schema?.columns || [];
        const data: Record<string, DataValue[]> = {};
        for (const col of columns) {
            data[col] = [];
        }
        return { columns, data, rowsProcessed: 0 };
    }

    let columnNames: string[];
    if (hasHeader || schema) {
        const dataObjects = parsedData as Record<string, unknown>[];
        columnNames = schema?.columns || Object.keys(dataObjects[0]);
    } else {
        const dataArrays = parsedData as string[][];
        const numCols = dataArrays[0]?.length || 0;
        columnNames = Array.from({ length: numCols }, (_, i) => `col_${i}`);
    }

    const stringData: (string | null)[][] = [];
    if (hasHeader || schema) {
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

    const columnTypes: Record<string, DType> = schema ? { ...schema.dtypes } : Object.fromEntries(columnNames.map((col) => [col, "string"]));

    for (const col of columnNames) {
        if (!columnTypes[col]) {
            columnTypes[col] = "string";
        }
    }

    const data: Record<string, DataValue[]> = {};
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

    const rowsProcessed = data[columnNames[0]]?.length || 0;

    return {
        columns: columnNames,
        dtypes: schema ? undefined : columnTypes,
        data,
        rowsProcessed,
    };
}

/**
 * Setup worker handler for batch processing
 */
setupWorker((data) => {
    const { seq, payload } = data;

    if (typeof seq !== "number" || seq < 0) {
        if (payload && typeof payload === "object") {
            if ("type" in payload) {
                const msg = payload as { type: string; payload?: unknown };
                if (msg.type === "INIT") {
                    const initPayload = msg.payload as InitPayload;
                    if (initPayload?.schema) {
                        schema = initPayload.schema;
                    }
                    if (initPayload) {
                        parseOptions = {
                            delimiter: initPayload.delimiter,
                            naValues: initPayload.naValues,
                            hasHeader: initPayload.hasHeader ?? true,
                            ...initPayload.parseOptions,
                        };
                    }
                    return;
                }
                if (msg.type === "CLOSE") {
                    // @ts-ignore - self.close exists in worker context
                    self.close();
                    return;
                }
            }
        }
        if (data && typeof data === "object" && "type" in data && !("seq" in data)) {
            const msg = data as { type: string; payload?: unknown };
            if (msg.type === "INIT") {
                const initPayload = msg.payload as InitPayload;
                if (initPayload?.schema) {
                    schema = initPayload.schema;
                }
                if (initPayload) {
                    parseOptions = {
                        delimiter: initPayload.delimiter,
                        naValues: initPayload.naValues,
                        hasHeader: initPayload.hasHeader ?? true,
                        ...initPayload.parseOptions,
                    };
                }
                return;
            }
            if (msg.type === "CLOSE") {
                // @ts-ignore - self.close exists in worker context
                self.close();
                return;
            }
        }
        return;
    }

    const batchPayload = payload as BatchPayload;

    if (!batchPayload || !batchPayload.linesBuffer) {
        throw new Error("BATCH message missing linesBuffer");
    }

    let linesBuffer: Uint8Array;
    if (batchPayload.linesBuffer instanceof ArrayBuffer) {
        linesBuffer = new Uint8Array(batchPayload.linesBuffer);
    } else if (batchPayload.linesBuffer instanceof Uint8Array) {
        linesBuffer = batchPayload.linesBuffer;
    } else {
        throw new Error("Invalid linesBuffer type");
    }

    const decoder = new TextDecoder();
    const text = decoder.decode(linesBuffer);
    const content = batchPayload.header ? batchPayload.header + "\n" + text : text;

    const result = parseCsvBatch(content, schema, parseOptions);

    return {
        seq,
        columns: result.columns,
        dtypes: result.dtypes,
        rowsProcessed: result.rowsProcessed,
        data: result.data,
    };
});
