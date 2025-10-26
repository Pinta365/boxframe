// @generated file from wasmbuild -- do not edit
// deno-lint-ignore-file
// deno-fmt-ignore-file

export function engine_create_series_f64(data: Float64Array): number;
export function engine_create_series_i32(data: Int32Array): number;
export function engine_free_series(series_id: number): void;
export function engine_free_series_i32(series_id: number): void;
export function engine_flush(): void;
export function engine_memory_usage(): number;
export function engine_series_count(): number;
export function engine_series_ptr_f64(series_id: number): number;
export function engine_series_len_f64(series_id: number): number;
export function engine_series_ptr_i32(series_id: number): number;
export function engine_series_len_i32(series_id: number): number;
export function engine_series_to_vec_f64(series_id: number): Float64Array;
export function engine_series_to_vec_i32(series_id: number): Int32Array;
export function engine_series_sum_f64(series_id: number): number;
export function engine_series_mean_f64(series_id: number): number;
export function engine_series_std_f64(series_id: number): number;
export function engine_series_min_f64(series_id: number): number;
export function engine_series_max_f64(series_id: number): number;
export function engine_series_count_f64(series_id: number): number;
/**
 * GroupBy sum using an existing registered f64 series and JSON keys
 * Returns a new series_id for the aggregated result (values sorted by key)
 */
export function engine_groupby_sum_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy mean using an existing registered f64 series and JSON keys
 */
export function engine_groupby_mean_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy count (non-null) using an existing registered f64 series and JSON keys
 */
export function engine_groupby_count_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy min using an existing registered f64 series and JSON keys
 */
export function engine_groupby_min_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy max using an existing registered f64 series and JSON keys
 */
export function engine_groupby_max_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy std using an existing registered f64 series and JSON keys (sample std, N-1)
 */
export function engine_groupby_std_f64(series_id: number, group_keys_json: string): number;
/**
 * GroupBy var using an existing registered f64 series and JSON keys (sample var, N-1)
 */
export function engine_groupby_var_f64(series_id: number, group_keys_json: string): number;
/**
 * Batch multi-aggregation for groupby on f64 series.
 * agg_mask bit layout (LSB -> MSB):
 * 1=sum, 2=mean, 4=count, 8=min, 16=max, 32=std, 64=var
 * Returns array of series ids in the above order for bits that are set.
 */
export function engine_groupby_multi_f64(series_id: number, group_keys_json: string, agg_mask: number): Uint32Array;
/**
 * Sort values (float64) ascending/descending, nulls last flag applies to NaN
 */
export function engine_sort_values_f64(series_id: number, ascending: number, nulls_last: number): number;
/**
 * Return sort indices (float64) for a registered series (no materialization)
 */
export function engine_sort_indices_f64(series_id: number, ascending: number, nulls_last: number): Uint32Array;
/**
 * Return sort indices by two registered f64 series (provided as two series ids)
 */
export function engine_sort_two_columns_indices_f64(
    series1_id: number,
    series2_id: number,
    asc1: number,
    asc2: number,
    nulls_last: number,
): Uint32Array;
/**
 * Return sort indices (int32) for a registered i32 series
 */
export function engine_sort_indices_i32(series_id: number, ascending: number, nulls_last: number): Uint32Array;
/**
 * Return sort indices by two registered i32 series
 */
export function engine_sort_two_columns_indices_i32(
    series1_id: number,
    series2_id: number,
    asc1: number,
    asc2: number,
    nulls_last: number,
): Uint32Array;
/**
 * Sort indices by two float64 columns (most common multi-column case)
 *
 * # Arguments
 * * `col1` - First column to sort by
 * * `col2` - Second column to sort by
 * * `asc1` - Whether first column should be ascending (1) or descending (0)
 * * `asc2` - Whether second column should be ascending (1) or descending (0)
 * * `nulls_last` - Whether to put null values at the end (1) or beginning (0)
 *
 * # Returns
 * * Array of indices sorted according to the multi-column criteria
 */
export function sort_two_columns_f64(col1: Float64Array, col2: Float64Array, asc1: number, asc2: number, nulls_last: number): Uint32Array;
/**
 * Sort indices by two int32 columns
 *
 * # Arguments
 * * `col1` - First column to sort by
 * * `col2` - Second column to sort by
 * * `asc1` - Whether first column should be ascending (1) or descending (0)
 * * `asc2` - Whether second column should be ascending (1) or descending (0)
 * * `nulls_last` - Whether to put null values at the end (1) or beginning (0)
 *
 * # Returns
 * * Array of indices sorted according to the multi-column criteria
 */
export function sort_two_columns_i32(col1: Int32Array, col2: Int32Array, asc1: number, asc2: number, nulls_last: number): Uint32Array;
/**
 * Sort indices by a single float64 column (optimized single-column version)
 *
 * # Arguments
 * * `data` - Float64 array to sort by
 * * `ascending` - Whether to sort in ascending order
 * * `nulls_last` - Whether to put null values at the end
 *
 * # Returns
 * * Array of indices sorted according to the column
 */
export function sort_single_column_f64(data: Float64Array, ascending: boolean, nulls_last: boolean): Uint32Array;
/**
 * Sort indices by a single int32 column (optimized single-column version)
 *
 * # Arguments
 * * `data` - Int32 array to sort by
 * * `ascending` - Whether to sort in ascending order
 * * `nulls_last` - Whether to put null values at the end
 *
 * # Returns
 * * Array of indices sorted according to the column
 */
export function sort_single_column_i32(data: Int32Array, ascending: boolean, nulls_last: boolean): Uint32Array;
/**
 * Filter float64 series using a boolean mask (1=true, 0=false)
 */
export function engine_filter_f64(series_id: number, mask: Uint8Array): number;
/**
 * High-performance filtering with boolean mask (using u8 array for WASM compatibility)
 */
export function filter_f64(data: Float64Array, mask: Uint8Array): Float64Array;
/**
 * High-performance vectorized sum
 */
export function sum_f64(data: Float64Array): number;
/**
 * High-performance vectorized mean
 */
export function mean_f64(data: Float64Array): number;
/**
 * High-performance vectorized standard deviation (sample)
 */
export function std_f64(data: Float64Array): number;
/**
 * High-performance vectorized min
 */
export function min_f64(data: Float64Array): number;
/**
 * High-performance vectorized max
 */
export function max_f64(data: Float64Array): number;
/**
 * Count non-null values
 */
export function count_non_null_f64(data: Float64Array): number;
/**
 * Check if values in an array are members of a given set (i32)
 *
 * # Arguments
 * * `data` - Array of i32 values to check
 * * `values` - Array of i32 values to check membership against
 *
 * # Returns
 * * Array of u8 values (0 = false, 1 = true) indicating membership
 */
export function isin_i32(data: Int32Array, values: Int32Array): Uint8Array;
/**
 * Check if values in an array are members of a given set (f64 with tolerance)
 * Note: For floating point numbers, we use a simple linear search with tolerance
 * since HashSet doesn't work well with floating point equality
 *
 * # Arguments
 * * `data` - Array of f64 values to check
 * * `values` - Array of f64 values to check membership against
 * * `tolerance` - Tolerance for floating point comparison (default: 1e-9)
 *
 * # Returns
 * * Array of u8 values (0 = false, 1 = true) indicating membership
 */
export function isin_f64(data: Float64Array, values: Float64Array, tolerance: number): Uint8Array;
/**
 * Check if values in an array are members of a given set (strings)
 * Note: This function takes string arrays as Vec<String> since &[String] is not supported by wasm-bindgen
 *
 * # Arguments
 * * `data` - Vector of string values to check
 * * `values` - Vector of string values to check membership against
 *
 * # Returns
 * * Array of u8 values (0 = false, 1 = true) indicating membership
 */
export function isin_string(data: string[], values: string[]): Uint8Array;
