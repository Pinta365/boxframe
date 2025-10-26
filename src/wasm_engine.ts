/**
 * WASM Engine
 * WASM engine functionality for BoxFrame
 */

import {
    count_non_null_f64,
    engine_create_series_f64,
    engine_create_series_i32,
    engine_filter_f64,
    engine_flush,
    engine_free_series,
    engine_free_series_i32,
    engine_groupby_count_f64,
    engine_groupby_max_f64,
    engine_groupby_mean_f64,
    engine_groupby_min_f64,
    engine_groupby_multi_f64,
    engine_groupby_std_f64,
    engine_groupby_sum_f64,
    engine_groupby_var_f64,
    engine_memory_usage,
    engine_series_count,
    engine_series_count_f64,
    engine_series_len_f64,
    engine_series_max_f64,
    engine_series_mean_f64,
    engine_series_min_f64,
    engine_series_ptr_f64,
    engine_series_std_f64,
    engine_series_sum_f64,
    engine_series_to_vec_f64,
    engine_series_to_vec_i32,
    engine_sort_indices_f64,
    engine_sort_indices_i32,
    engine_sort_two_columns_indices_f64,
    engine_sort_two_columns_indices_i32,
    engine_sort_values_f64,
    isin_f64,
    isin_i32,
    isin_string,
} from "./wasm_wrapper.ts";

export class WasmEngine {
    private static _instance: WasmEngine | null = null;

    static get instance(): WasmEngine {
        if (!WasmEngine._instance) {
            WasmEngine._instance = new WasmEngine();
        }
        return WasmEngine._instance;
    }

    registerSeriesF64(values: Float64Array): number {
        return engine_create_series_f64(values);
    }

    freeSeries(id: number): void {
        engine_free_series(id);
    }

    flush(): void {
        engine_flush();
    }

    getSeriesPtr(id: number): number {
        return engine_series_ptr_f64(id);
    }

    getSeriesLen(id: number): number {
        return engine_series_len_f64(id);
    }

    groupbySumF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_sum_f64(seriesId, groupKeysJson);
    }

    groupbyMeanF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_mean_f64(seriesId, groupKeysJson);
    }

    groupbyCountF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_count_f64(seriesId, groupKeysJson);
    }

    groupbyMinF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_min_f64(seriesId, groupKeysJson);
    }

    groupbyMaxF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_max_f64(seriesId, groupKeysJson);
    }

    groupbyStdF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_std_f64(seriesId, groupKeysJson);
    }

    groupbyVarF64(seriesId: number, groupKeysJson: string): number {
        return engine_groupby_var_f64(seriesId, groupKeysJson);
    }

    groupbyMultiF64(seriesId: number, groupKeysJson: string, mask: number): number[] {
        return Array.from(engine_groupby_multi_f64(seriesId, groupKeysJson, mask >>> 0));
    }

    sortValuesF64(seriesId: number, ascending: boolean, nullsLast: boolean): number {
        return engine_sort_values_f64(seriesId, ascending ? 1 : 0, nullsLast ? 1 : 0);
    }

    sortIndicesF64(seriesId: number, ascending: boolean, nullsLast: boolean): number[] {
        return Array.from(engine_sort_indices_f64(seriesId, ascending ? 1 : 0, nullsLast ? 1 : 0));
    }

    filterF64(seriesId: number, mask: Uint8Array): number {
        return engine_filter_f64(seriesId, mask);
    }

    seriesToVecF64(seriesId: number): number[] {
        return Array.from(engine_series_to_vec_f64(seriesId));
    }

    seriesSumF64(seriesId: number): number {
        return engine_series_sum_f64(seriesId);
    }

    seriesMeanF64(seriesId: number): number {
        return engine_series_mean_f64(seriesId);
    }

    seriesStdF64(seriesId: number): number {
        return engine_series_std_f64(seriesId);
    }

    seriesMinF64(seriesId: number): number {
        return engine_series_min_f64(seriesId);
    }

    seriesMaxF64(seriesId: number): number {
        return engine_series_max_f64(seriesId);
    }

    seriesCountF64(seriesId: number): number {
        return engine_series_count_f64(seriesId);
    }

    sortTwoColumnsIndicesF64(series1Id: number, series2Id: number, asc1: boolean, asc2: boolean, nullsLast: boolean): number[] {
        return Array.from(engine_sort_two_columns_indices_f64(series1Id, series2Id, asc1 ? 1 : 0, asc2 ? 1 : 0, nullsLast ? 1 : 0));
    }

    registerSeriesI32(values: Int32Array): number {
        return engine_create_series_i32(values);
    }
    freeSeriesI32(id: number): void {
        engine_free_series_i32(id);
    }
    seriesToVecI32(seriesId: number): number[] {
        return Array.from(engine_series_to_vec_i32(seriesId));
    }
    sortIndicesI32(seriesId: number, ascending: boolean, nullsLast: boolean): number[] {
        return Array.from(engine_sort_indices_i32(seriesId, ascending ? 1 : 0, nullsLast ? 1 : 0));
    }
    sortTwoColumnsIndicesI32(series1Id: number, series2Id: number, asc1: boolean, asc2: boolean, nullsLast: boolean): number[] {
        return Array.from(engine_sort_two_columns_indices_i32(series1Id, series2Id, asc1 ? 1 : 0, asc2 ? 1 : 0, nullsLast ? 1 : 0));
    }

    static isinF64(data: Float64Array, values: Float64Array, tolerance: number = 1e-9): Uint8Array {
        return isin_f64(data, values, tolerance);
    }
    static isinI32(data: Int32Array, values: Int32Array): Uint8Array {
        return isin_i32(data, values);
    }
    static isinString(data: string[], values: string[]): Uint8Array {
        return isin_string(data, values);
    }

    static getMemoryUsage(): number {
        return engine_memory_usage();
    }

    countNonNullF64(data: Float64Array): number {
        return count_non_null_f64(data) as number;
    }

    static getSeriesCount(): number {
        return engine_series_count();
    }

    static countNonNullF64(data: Float64Array): number {
        return count_non_null_f64(data) as number;
    }
}
