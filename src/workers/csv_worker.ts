/**
 * CSV Worker Module
 * Handles CSV parsing in a worker context
 */

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

/**
 * Worker message types
 */
type WorkerMessage =
    | { type: "INIT"; payload: InitPayload }
    | { type: "BATCH"; payload: BatchPayload }
    | { type: "CLOSE"; payload: {} };

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

    return {
        columns: columnNames,
        dtypes: schema ? undefined : columnTypes,
        data,
        rowsProcessed: data[columnNames[0]]?.length || 0,
    };
}

/**
 * Worker message handler
 */
// @ts-ignore - self is available in worker context
self.onmessage = async (ev: MessageEvent<WorkerMessage>) => {
    try {
        let type: string;
        let payload: unknown;
        let seq: number | undefined;

        if (ev.data && typeof ev.data === "object") {
            if ("type" in ev.data && "payload" in ev.data) {
                type = ev.data.type as string;
                payload = ev.data.payload;
                if ("seq" in ev.data) {
                    seq = ev.data.seq as number;
                }
            } else {
                type = (ev.data as any).type || "";
                payload = ev.data;
                seq = (ev.data as any).seq;
            }
        } else {
            throw new Error(`Invalid message format: ${typeof ev.data}`);
        }

        switch (type) {
            case "INIT": {
                const initPayload = payload as InitPayload;
                if (initPayload.schema) {
                    schema = initPayload.schema;
                }
                parseOptions = {
                    delimiter: initPayload.delimiter,
                    naValues: initPayload.naValues,
                    hasHeader: initPayload.hasHeader ?? true,
                    ...initPayload.parseOptions,
                };
                return;
            }

            case "BATCH": {
                const batchPayload = payload as BatchPayload;
                const batchSeq = seq ?? batchPayload.seq;
                const { header, linesBuffer } = batchPayload;

                if (!linesBuffer) {
                    throw new Error("BATCH message missing linesBuffer");
                }

                const decoder = new TextDecoder();
                const text = decoder.decode(linesBuffer);
                const content = header ? header + "\n" + text : text;

                const result = parseCsvBatch(content, schema, parseOptions);

                // @ts-ignore - self is available in worker context
                self.postMessage({
                    type: "RESULT",
                    seq: batchSeq,
                    columns: result.columns,
                    dtypes: result.dtypes,
                    rowsProcessed: result.rowsProcessed,
                    data: result.data,
                });
                return;
            }

            case "CLOSE": {
                // @ts-ignore - self is available in worker context
                self.close();
                return;
            }

            default: {
                throw new Error(`Unknown message type: ${type}`);
            }
        }
    } catch (error) {
        // @ts-ignore - self is available in worker context
        self.postMessage({
            type: "ERROR",
            seq: (ev.data as any)?.seq ?? (ev.data as any)?.payload?.seq ?? -1,
            message: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
        });
    }
};
