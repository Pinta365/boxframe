/**
 * Core data types for BoxFrame
 */

/**
 * Supported data types for Series and DataFrame columns
 */
export type DType = "int32" | "float64" | "string" | "bool" | "datetime";

/**
 * Index type - can be string labels or numeric indices
 */
export type Index = string | number;

/**
 * Generic data value type - represents any value that can be stored in a Series
 */
export type DataValue = string | number | boolean | Date | null | undefined;

/**
 * Row data type for DataFrame operations
 */
export type RowData = Record<string, DataValue>;

/**
 * Column data type for DataFrame creation
 */
export type ColumnData = DataValue[];

/**
 * DataFrame input data type
 */
export type DataFrameInput = Record<string, ColumnData> | Map<string, ColumnData>;

/**
 * Generic data array type that can hold different data types
 */
export type DataArray<T> = T[] | Int32Array | Float64Array | Uint8Array;

/**
 * Options for CSV reading
 */
export interface CsvOptions {
    delimiter?: string;
    hasHeader?: boolean;
    skipRows?: number;
    encoding?: string;
    naValues?: string[];
}

/**
 * Options for JSON reading
 */
export interface JsonOptions {
    orient?: "records" | "index" | "values" | "columns";
    encoding?: string;
    index?: Index[];
}

/**
 * Options for merge operations
 */
export interface MergeOptions {
    how?: "inner" | "left" | "right" | "outer";
    on?: string | string[];
    leftOn?: string | string[];
    rightOn?: string | string[];
    suffixes?: [string, string];
}

/**
 * GroupBy key type - can be function, array, or undefined (for index grouping)
 */
export type GroupByKey = ((value: DataValue, index: number) => string | number) | (string | number | null)[] | undefined;

/**
 * Options for groupby operations
 */
export interface GroupByOptions {
    as_index?: boolean;
    sort?: boolean;
    dropna?: boolean;
}

/**
 * Aggregation function type
 */
export type AggFunc = "sum" | "mean" | "count" | "size" | "min" | "max" | "std" | "var" | "first" | "last";

/**
 * Aggregation specification for groupby operations
 */
export type AggSpec = AggFunc | { [column: string]: AggFunc | AggFunc[] };

/**
 * Rolling window options
 */
export interface RollingOptions {
    window: number;
    min_periods?: number;
    center?: boolean;
    win_type?: string;
}

/**
 * Expanding window options
 */
export interface ExpandingOptions {
    min_periods?: number;
}

/**
 * Series constructor options
 */
export interface SeriesOptions {
    name?: string;
    dtype?: DType;
    index?: Index[];
}

/**
 * DataFrame constructor options
 */
export interface DataFrameOptions {
    index?: Index[];
    columns?: string[];
}

/**
 * Type guard to check if a value is a valid DType
 */
export function isDType(value: string): value is DType {
    return ["int32", "float64", "string", "bool", "datetime"].includes(value);
}

/**
 * Infer the DType from a JavaScript value
 */
export function inferDType(value: DataValue): DType {
    if (typeof value === "number") {
        // Special float values are always float64
        if (
            value === Number.EPSILON ||
            value === Number.POSITIVE_INFINITY ||
            value === Number.NEGATIVE_INFINITY ||
            Number.isNaN(value)
        ) {
            return "float64";
        }

        // Numbers outside the safe integer range are float64 (except -0)
        if (!Number.isSafeInteger(value) && !Object.is(value, -0)) {
            return "float64";
        }

        // Check if it's a float by looking for decimal places in string representation
        const str = value.toString();
        if (str.includes(".") || str.includes("e") || str.includes("E")) {
            return "float64";
        }

        // Otherwise, it's an integer
        return "int32";
    }
    if (typeof value === "string") {
        return "string";
    }
    if (typeof value === "boolean") {
        return "bool";
    }
    if (value instanceof Date) {
        return "datetime";
    }
    return "string"; // default fallback
}

/**
 * Convert a JavaScript value to the appropriate typed array format
 */
export function toTypedArray<T>(data: T[], dtype: DType): DataArray<T> {
    switch (dtype) {
        case "int32":
            // Return regular array if nulls are present
            if ((data as DataValue[]).some((val) => val === null || val === undefined)) {
                return (data as DataValue[]).map((val) => val === null || val === undefined ? null : val) as DataArray<T>;
            }
            return new Int32Array((data as number[]).map((val) => val === null || val === undefined ? 0 : val));
        case "float64":
            return new Float64Array((data as number[]).map((val) => val === null || val === undefined ? NaN : val));
        case "bool":
            return (data as boolean[]).map((b) => b === null || b === undefined ? false : b) as DataArray<T>;
        case "datetime":
            return (data as Date[]).map((d) => d === null || d === undefined ? null : d) as DataArray<T>;
        case "string":
        default:
            return data;
    }
}

/**
 * Convert typed array back to JavaScript values
 */
export function fromTypedArray<T>(data: DataArray<T>, dtype: DType): T[] {
    if (Array.isArray(data)) {
        return data;
    }

    switch (dtype) {
        case "int32":
            return Array.from(data as Int32Array).map((val) => val === 0 ? 0 : val) as T[];
        case "float64":
            return Array.from(data as Float64Array).map((val) => isNaN(val) ? null : val) as T[];
        case "bool":
            return Array.from(data as Uint8Array).map((b) => b === 1) as T[];
        case "datetime":
            return Array.from(data as Float64Array).map((t) => isNaN(t) ? null : new Date(t)) as T[];
        default:
            return Array.from(data) as T[];
    }
}

/**
 * Query expression node types for DataFrame.query() method
 */
export type QueryNode = QueryCondition | QueryLogical;

/**
 * Simple condition in a query expression
 */
export interface QueryCondition {
    type: "condition";
    column: string;
    operator: ">" | "<" | ">=" | "<=" | "==" | "!=";
    value: DataValue;
}

/**
 * Logical operation in a query expression
 */
export interface QueryLogical {
    type: "and" | "or";
    left: QueryNode;
    right: QueryNode;
}

/**
 * Extended globalThis interface to include BoxFrame-specific properties
 */
declare global {
    var DF_USE_WASM_ENGINE: boolean | undefined;
}

/**
 * Helper function to check if WASM engine is enabled
 * Defaults to enabled unless explicitly disabled
 */
export function isWasmEngineEnabled(): boolean {
    return globalThis.DF_USE_WASM_ENGINE !== false;
}
