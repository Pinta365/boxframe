import { Series } from "./series.ts";
import { WasmEngine } from "./wasm_engine.ts";
import type { AggFunc, AggSpec, DataValue, DType, GroupByKey, GroupByOptions, Index } from "./types.ts";
import { isWasmEngineEnabled } from "./types.ts";
import { calculateCount, calculateMax, calculateMean, calculateMin, calculateStd, calculateSum, calculateVar } from "./groupby_utils.ts";

/**
 * GroupBy - Represents grouped data from a Series
 *
 * This class provides aggregation operations on grouped data
 */
export class GroupBy<T extends DataValue = DataValue> {
    private _groups: Map<string, number[]>;
    private groupKeys: string[];
    private fullGroupKeys: string[];
    private originalData: Series<T>;
    private options: GroupByOptions;

    constructor(
        data: Series<T>,
        groupKeys: string[],
        groups: Map<string, number[]>,
        fullGroupKeys: string[],
        options: GroupByOptions = {},
    ) {
        this.originalData = data;
        this.groupKeys = groupKeys;
        this.fullGroupKeys = fullGroupKeys;
        this._groups = groups;
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
     * Get a specific group as a Series
     */
    getGroup(name: string): Series<T> {
        const indices = this._groups.get(name);
        if (!indices) {
            throw new Error(`Group '${name}' not found`);
        }
        const values = this.originalData.values;
        const groupData = indices.map((i) => values[i]);
        const groupIndex = indices.map((i) => this.originalData.index[i]);

        return new Series(groupData, {
            name: `${this.originalData.name}_group_${name}`,
            index: groupIndex,
            dtype: this.originalData.dtype,
        });
    }

    /**
     * Get the size of each group
     */
    size(): Series<number> {
        const sizes = this.groupKeys.map((key) => this._groups.get(key)!.length);
        return new Series(sizes, {
            name: `${this.originalData.name}_size`,
            index: this.groupKeys,
            dtype: "int32",
        });
    }

    /**
     * Apply aggregation function
     */
    agg(aggregation: AggSpec): Series {
        if (typeof aggregation === "string") {
            return this._singleAggregation(aggregation);
        } else {
            // For Series GroupBy, we only support single function aggregations
            // Multiple functions would require returning a DataFrame, which is not supported
            const funcs = Object.values(aggregation) as (AggFunc | AggFunc[])[];
            const flattenedFuncs: AggFunc[] = [];

            for (const funcOrFuncs of funcs) {
                if (Array.isArray(funcOrFuncs)) {
                    flattenedFuncs.push(...funcOrFuncs);
                } else {
                    flattenedFuncs.push(funcOrFuncs);
                }
            }

            if (flattenedFuncs.length > 1) {
                throw new Error("Series GroupBy.agg() only supports single aggregation functions. Use DataFrame GroupBy for multiple functions.");
            }

            const uniqueFuncs = Array.from(new Set(flattenedFuncs));
            const canUseEngine = (this.originalData.dtype === "float64") && isWasmEngineEnabled();
            if (!canUseEngine) {
                return this._singleAggregation(uniqueFuncs[0]);
            }
            try {
                const engine = WasmEngine.instance;
                const data = new Float64Array(this.originalData.values as number[]);
                const seriesId = engine.registerSeriesF64(data);
                const groupKeysJson = JSON.stringify(this.groupKeys);
                let mask = 0;
                for (const f of uniqueFuncs) {
                    switch (f) {
                        case "sum":
                            mask |= 1;
                            break;
                        case "mean":
                            mask |= 2;
                            break;
                        case "count":
                            mask |= 4;
                            break;
                        case "min":
                            mask |= 8;
                            break;
                        case "max":
                            mask |= 16;
                            break;
                        case "std":
                            mask |= 32;
                            break;
                        case "var":
                            mask |= 64;
                            break;
                        default:
                            break;
                    }
                }
                const ids = engine.groupbyMultiF64(seriesId, groupKeysJson, mask);
                engine.freeSeries(seriesId);
                const first = uniqueFuncs[0];
                const order: AggFunc[] = ["sum", "mean", "count", "min", "max", "std", "var"];
                const idx = order.indexOf(first);
                const selectedIds: number[] = [];
                let bit = 1;
                let j = 0;
                for (const _name of order) {
                    if ((mask & bit) !== 0) {
                        selectedIds.push(ids[j]);
                        j++;
                    }
                    bit <<= 1;
                }
                const resultId = selectedIds[idx >= 0 ? idx : 0];
                const resultValues = engine.seriesToVecF64(resultId);
                const s = new Series(resultValues, {
                    name: `${this.originalData.name}_${first}`,
                    index: this.groupKeys,
                    dtype: (first === "count" ? "int32" : "float64"),
                });
                for (const id of ids) engine.freeSeries(id);
                return s;
            } catch (_e) {
                return this._singleAggregation(uniqueFuncs[0]);
            }
        }
    }

    /**
     * Calculate sum for each group
     */
    sum(): Series {
        return this._singleAggregation("sum");
    }

    /**
     * Calculate mean for each group
     */
    mean(): Series {
        return this._singleAggregation("mean");
    }

    /**
     * Calculate count for each group
     */
    count(): Series {
        return this._singleAggregation("count");
    }

    /**
     * Calculate minimum for each group
     */
    min(): Series {
        return this._singleAggregation("min");
    }

    /**
     * Calculate maximum for each group
     */
    max(): Series {
        return this._singleAggregation("max");
    }

    /**
     * Calculate standard deviation for each group
     */
    std(): Series {
        return this._singleAggregation("std");
    }

    /**
     * Calculate variance for each group
     */
    var(): Series {
        return this._singleAggregation("var");
    }

    /**
     * Get first value for each group
     */
    first(): Series {
        return this._singleAggregation("first");
    }

    /**
     * Get last value for each group
     */
    last(): Series {
        return this._singleAggregation("last");
    }

    /**
     * Internal method to perform single aggregation
     */
    private _singleAggregation(func: AggFunc): Series {
        if (this.originalData.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const values = (this.originalData.values as number[]).map((v) => v === null || v === undefined ? NaN : v);
            const data = new Float64Array(values);
            const seriesId = engine.registerSeriesF64(data);
            const groupKeysJson = JSON.stringify(this.fullGroupKeys);

            if (func === "first" || func === "last") {
                engine.freeSeries(seriesId);
            } else {
                let resultId: number;
                switch (func) {
                    case "sum":
                        resultId = engine.groupbySumF64(seriesId, groupKeysJson);
                        break;
                    case "mean":
                        resultId = engine.groupbyMeanF64(seriesId, groupKeysJson);
                        break;
                    case "count":
                        resultId = engine.groupbyCountF64(seriesId, groupKeysJson);
                        break;
                    case "min":
                        resultId = engine.groupbyMinF64(seriesId, groupKeysJson);
                        break;
                    case "max":
                        resultId = engine.groupbyMaxF64(seriesId, groupKeysJson);
                        break;
                    case "std":
                        resultId = engine.groupbyStdF64(seriesId, groupKeysJson);
                        break;
                    case "var":
                        resultId = engine.groupbyVarF64(seriesId, groupKeysJson);
                        break;
                    default:
                        engine.freeSeries(seriesId);
                        throw new Error(`Unsupported aggregation function: ${func}`);
                }

                engine.freeSeries(seriesId);
                const resultValues = engine.seriesToVecF64(resultId);
                if (resultValues.length === 0) {
                    engine.freeSeries(resultId);
                    return new Series([], {
                        name: `${this.originalData.name}_${func}`,
                        index: [],
                        dtype: func === "count" ? "int32" : "float64",
                    });
                }
                const dtype = func === "count" ? "int32" : "float64";
                const s = new Series(resultValues, { name: `${this.originalData.name}_${func}`, index: this.groupKeys, dtype });
                engine.freeSeries(resultId);
                return s;
            }
        }
        const results: DataValue[] = [];
        const resultIndex: Index[] = [];

        const values = this.originalData.values as DataValue[];

        for (const groupKey of this.groupKeys) {
            const indices = this._groups.get(groupKey)!;
            const groupValues = indices.map((i) => values[i]);

            let result: DataValue;
            switch (func) {
                case "sum":
                    result = calculateSum(groupValues);
                    break;
                case "mean":
                    result = calculateMean(groupValues);
                    break;
                case "count":
                    result = calculateCount(groupValues);
                    break;
                case "size":
                    result = indices.length;
                    break;
                case "min":
                    result = calculateMin(groupValues);
                    break;
                case "max":
                    result = calculateMax(groupValues);
                    break;
                case "std":
                    result = calculateStd(groupValues);
                    break;
                case "var":
                    result = calculateVar(groupValues);
                    break;
                case "first":
                    result = groupValues[0];
                    break;
                case "last":
                    result = groupValues[groupValues.length - 1];
                    break;
                default:
                    throw new Error(`Unsupported aggregation function: ${func}`);
            }

            results.push(result);
            resultIndex.push(groupKey);
        }

        return new Series(results, {
            name: `${this.originalData.name}_${func}`,
            index: resultIndex,
            dtype: this._inferResultDType(func),
        });
    }

    /**
     * Infer the result data type based on aggregation function
     */
    private _inferResultDType(func: AggFunc): DType {
        switch (func) {
            case "sum":
            case "mean":
            case "std":
            case "var":
            case "min":
            case "max":
                return "float64";
            case "count":
            case "size":
                return "int32";
            case "first":
            case "last":
                return this.originalData.dtype;
            default:
                return "string";
        }
    }
}

/**
 * Create groups from Series data based on grouping key
 */
export function createGroups<T extends DataValue>(
    data: Series<T>,
    by: GroupByKey,
    options: GroupByOptions = {},
): { groups: Map<string, number[]>; groupKeys: string[]; fullGroupKeys: string[] } {
    const groups = new Map<string, number[]>();
    const fullGroupKeys: string[] = [];
    const dropna = options.dropna !== false;

    if (typeof by === "function") {
        for (let i = 0; i < data.length; i++) {
            const groupKey = by(data.values[i] as DataValue, i);
            const keyStr = String(groupKey);

            if (dropna && (groupKey === null || groupKey === undefined)) {
                fullGroupKeys.push("");
                continue;
            }

            fullGroupKeys.push(keyStr);
            if (!groups.has(keyStr)) {
                groups.set(keyStr, []);
            }
            groups.get(keyStr)!.push(i);
        }
    } else if (Array.isArray(by)) {
        if (by.length !== data.length) {
            throw new Error(`Grouping array length (${by.length}) must match Series length (${data.length})`);
        }

        for (let i = 0; i < by.length; i++) {
            const groupKey = by[i];
            const keyStr = String(groupKey);

            if (dropna && (groupKey === null || groupKey === undefined)) {
                fullGroupKeys.push("");
                continue;
            }

            fullGroupKeys.push(keyStr);
            if (!groups.has(keyStr)) {
                groups.set(keyStr, []);
            }
            groups.get(keyStr)!.push(i);
        }
    } else if (by === undefined) {
        for (let i = 0; i < data.length; i++) {
            const groupKey = data.index[i];
            const keyStr = String(groupKey);

            if (dropna && (groupKey === null || groupKey === undefined)) {
                fullGroupKeys.push("");
                continue;
            }

            fullGroupKeys.push(keyStr);
            if (!groups.has(keyStr)) {
                groups.set(keyStr, []);
            }
            groups.get(keyStr)!.push(i);
        }
    } else {
        throw new Error(`Unsupported groupby key type: ${typeof by}`);
    }

    const groupKeys = Array.from(groups.keys());
    if (options.sort !== false) {
        groupKeys.sort();
    }

    return { groups, groupKeys, fullGroupKeys };
}
