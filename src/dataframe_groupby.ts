import { DataFrame } from "./dataframe.ts";
import { Series } from "./series.ts";
import type { AggFunc, AggSpec, DataValue, GroupByOptions, Index } from "./types.ts";
import {
    calculateCountByIndices,
    calculateMaxByIndices,
    calculateMeanByIndices,
    calculateMinByIndices,
    calculateStdByIndices,
    calculateSumByIndices,
    calculateVarByIndices,
} from "./groupby_utils.ts";

/**
 * DataFrameGroupBy - Represents grouped data from a DataFrame
 *
 * This class provides aggregation operations on grouped DataFrame data
 */
export class DataFrameGroupBy {
    private _groups: Map<string, number[]>;
    private groupKeys: string[];
    private originalData: DataFrame;
    private groupColumns: string[];
    private options: GroupByOptions;

    constructor(
        data: DataFrame,
        groupColumns: string[],
        groups: Map<string, number[]>,
        options: GroupByOptions = {},
    ) {
        this.originalData = data;
        this.groupColumns = groupColumns;
        this._groups = groups;
        this.groupKeys = Array.from(groups.keys());
        this.options = {
            as_index: true,
            sort: true,
            dropna: true,
            ...options,
        };
    }

    /**
     * Get the groups as a Map
     */
    groups(): Map<string, number[]> {
        return new Map(this._groups);
    }

    /**
     * Get a specific group as a DataFrame
     */
    getGroup(name: string): DataFrame {
        const indices = this._groups.get(name);
        if (!indices) {
            throw new Error(`Group '${name}' not found`);
        }

        const newData = new Map<string, DataValue[]>();
        const newIndex: Index[] = [];

        // Extract data for the group
        for (const [colName, series] of this.originalData.data) {
            const groupValues = indices.map((i) => series.values[i]);
            newData.set(colName, groupValues);
        }

        for (const i of indices) {
            newIndex.push(this.originalData.index[i]);
        }

        return new DataFrame(newData, {
            index: newIndex,
            columns: this.originalData.columns,
        });
    }

    /**
     * Get the size of each group
     */
    size(): Series<number> {
        const sizes = this.groupKeys.map((key) => this._groups.get(key)!.length);
        return new Series(sizes, {
            name: "size",
            index: this.groupKeys,
            dtype: "int32",
        });
    }

    /**
     * Apply aggregation functions
     */
    agg(aggregation: AggSpec): DataFrame {
        if (typeof aggregation === "string") {
            return this._singleAggregation(aggregation);
        } else {
            return this._multipleAggregation(aggregation);
        }
    }

    /**
     * Calculate sum for each group
     */
    sum(): DataFrame {
        return this._singleAggregation("sum");
    }

    /**
     * Calculate mean for each group
     */
    mean(): DataFrame {
        return this._singleAggregation("mean");
    }

    /**
     * Calculate count for each group
     */
    count(): DataFrame {
        return this._singleAggregation("count");
    }

    /**
     * Calculate minimum for each group
     */
    min(): DataFrame {
        return this._singleAggregation("min");
    }

    /**
     * Calculate maximum for each group
     */
    max(): DataFrame {
        return this._singleAggregation("max");
    }

    /**
     * Calculate standard deviation for each group
     */
    std(): DataFrame {
        return this._singleAggregation("std");
    }

    /**
     * Calculate variance for each group
     */
    var(): DataFrame {
        return this._singleAggregation("var");
    }

    /**
     * Get first value for each group
     */
    first(): DataFrame {
        return this._singleAggregation("first");
    }

    /**
     * Get last value for each group
     */
    last(): DataFrame {
        return this._singleAggregation("last");
    }

    /**
     * Internal method to perform single aggregation across all columns
     */
    private _singleAggregation(func: AggFunc): DataFrame {
        const newData = new Map<string, DataValue[]>();
        const resultIndex = this.groupKeys;

        for (const [colName, series] of this.originalData.data) {
            const columnResults: DataValue[] = [];
            const values = series.values;

            for (const groupKey of this.groupKeys) {
                const indices = this._groups.get(groupKey)!;

                let result: DataValue;
                switch (func) {
                    case "sum":
                        result = calculateSumByIndices(values, indices);
                        break;
                    case "mean":
                        result = calculateMeanByIndices(values, indices);
                        break;
                    case "count":
                        result = calculateCountByIndices(values, indices);
                        break;
                    case "size":
                        result = indices.length;
                        break;
                    case "min":
                        result = calculateMinByIndices(values, indices);
                        break;
                    case "max":
                        result = calculateMaxByIndices(values, indices);
                        break;
                    case "std":
                        result = calculateStdByIndices(values, indices);
                        break;
                    case "var":
                        result = calculateVarByIndices(values, indices);
                        break;
                    case "first":
                        result = values[indices[0]];
                        break;
                    case "last":
                        result = values[indices[indices.length - 1]];
                        break;
                    default:
                        throw new Error(`Unsupported aggregation function: ${func}`);
                }

                columnResults.push(result);
            }

            newData.set(colName, columnResults);
        }

        return new DataFrame(newData, {
            index: resultIndex,
            columns: this.originalData.columns,
        });
    }

    /**
     * Internal method to perform multiple aggregations
     */
    private _multipleAggregation(aggregation: { [column: string]: AggFunc | AggFunc[] }): DataFrame {
        const newData = new Map<string, DataValue[]>();
        const resultIndex = this.groupKeys;
        const resultColumns: string[] = [];

        const seriesData = new Map<string, DataValue[]>();
        for (const [colName, _funcOrFuncs] of Object.entries(aggregation)) {
            const series = this.originalData.data.get(colName);
            if (!series) {
                throw new Error(`Column '${colName}' not found`);
            }
            seriesData.set(colName, series.values);
        }

        for (const [colName, funcOrFuncs] of Object.entries(aggregation)) {
            const values = seriesData.get(colName)!;
            const functions = Array.isArray(funcOrFuncs) ? funcOrFuncs : [funcOrFuncs];

            for (const func of functions) {
                const columnResults: DataValue[] = [];

                for (const groupKey of this.groupKeys) {
                    const indices = this._groups.get(groupKey)!;

                    let result: DataValue;
                    switch (func) {
                        case "sum":
                            result = calculateSumByIndices(values, indices);
                            break;
                        case "mean":
                            result = calculateMeanByIndices(values, indices);
                            break;
                        case "count":
                            result = calculateCountByIndices(values, indices);
                            break;
                        case "size":
                            result = indices.length;
                            break;
                        case "min":
                            result = calculateMinByIndices(values, indices);
                            break;
                        case "max":
                            result = calculateMaxByIndices(values, indices);
                            break;
                        case "std":
                            result = calculateStdByIndices(values, indices);
                            break;
                        case "var":
                            result = calculateVarByIndices(values, indices);
                            break;
                        case "first":
                            result = values[indices[0]];
                            break;
                        case "last":
                            result = values[indices[indices.length - 1]];
                            break;
                        default:
                            throw new Error(`Unsupported aggregation function: ${func}`);
                    }

                    columnResults.push(result);
                }

                const resultColName = `${colName}_${func}`;
                newData.set(resultColName, columnResults);
                resultColumns.push(resultColName);
            }
        }

        return new DataFrame(newData, {
            index: resultIndex,
            columns: resultColumns,
        });
    }
}

/**
 * Create groups from DataFrame data based on grouping columns
 */
export function createDataFrameGroups(
    data: DataFrame,
    groupColumns: string[],
    options: GroupByOptions = {},
): { groups: Map<string, number[]>; groupKeys: string[] } {
    const groups = new Map<string, number[]>();
    const dropna = options.dropna !== false;

    for (const col of groupColumns) {
        if (!data.data.has(col)) {
            throw new Error(`Column '${col}' not found`);
        }
    }

    for (let i = 0; i < data.length; i++) {
        const groupKeyParts: string[] = [];
        let hasNull = false;

        for (const col of groupColumns) {
            const series = data.data.get(col)!;
            const value = series.values[i];

            if (value === null || value === undefined) {
                hasNull = true;
            }
            groupKeyParts.push(String(value));
        }

        if (dropna && hasNull) {
            continue;
        }

        const groupKey = groupKeyParts.join("|");

        if (!groups.has(groupKey)) {
            groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(i);
    }

    const groupKeys = Array.from(groups.keys());
    if (options.sort !== false) {
        groupKeys.sort();
    }

    return { groups, groupKeys };
}
