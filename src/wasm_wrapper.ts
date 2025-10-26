/**
 * WASM Wrapper
 * Handles WASM module loading, fallback behavior, and conditional execution
 */

import { isWasmEngineEnabled } from "./types.ts";

// Create fallback functions that throw errors when WASM is not available
function createWasmFallback(functionName: string) {
    return function (..._args: unknown[]) {
        throw new Error(`WASM engine is disabled. Function ${functionName} is not available.`);
    };
}

// Try to import WASM functions, fall back to stubs if disabled or failed
let wasmFunctions: typeof import("../wasm/lib/wasm_frame.js") | undefined;

if (isWasmEngineEnabled()) {
    try {
        wasmFunctions = await import("../wasm/lib/wasm_frame.js");
    } catch (error) {
        console.warn("Failed to load WASM module, falling back to JavaScript implementation:", error);
        // wasmFunctions remains empty, will use fallbacks
    }
}

// Export WASM functions or fallbacks
export const engine_create_series_f64 = wasmFunctions?.engine_create_series_f64 || createWasmFallback("engine_create_series_f64");
export const engine_create_series_i32 = wasmFunctions?.engine_create_series_i32 || createWasmFallback("engine_create_series_i32");
export const engine_filter_f64 = wasmFunctions?.engine_filter_f64 || createWasmFallback("engine_filter_f64");
export const engine_flush = wasmFunctions?.engine_flush || createWasmFallback("engine_flush");
export const engine_free_series = wasmFunctions?.engine_free_series || createWasmFallback("engine_free_series");
export const engine_free_series_i32 = wasmFunctions?.engine_free_series_i32 || createWasmFallback("engine_free_series_i32");
export const engine_groupby_count_f64 = wasmFunctions?.engine_groupby_count_f64 || createWasmFallback("engine_groupby_count_f64");
export const engine_groupby_max_f64 = wasmFunctions?.engine_groupby_max_f64 || createWasmFallback("engine_groupby_max_f64");
export const engine_groupby_mean_f64 = wasmFunctions?.engine_groupby_mean_f64 || createWasmFallback("engine_groupby_mean_f64");
export const engine_groupby_min_f64 = wasmFunctions?.engine_groupby_min_f64 || createWasmFallback("engine_groupby_min_f64");
export const engine_groupby_multi_f64 = wasmFunctions?.engine_groupby_multi_f64 || createWasmFallback("engine_groupby_multi_f64");
export const engine_groupby_std_f64 = wasmFunctions?.engine_groupby_std_f64 || createWasmFallback("engine_groupby_std_f64");
export const engine_groupby_sum_f64 = wasmFunctions?.engine_groupby_sum_f64 || createWasmFallback("engine_groupby_sum_f64");
export const engine_groupby_var_f64 = wasmFunctions?.engine_groupby_var_f64 || createWasmFallback("engine_groupby_var_f64");
export const engine_series_count_f64 = wasmFunctions?.engine_series_count_f64 || createWasmFallback("engine_series_count_f64");
export const engine_series_len_f64 = wasmFunctions?.engine_series_len_f64 || createWasmFallback("engine_series_len_f64");
export const engine_series_max_f64 = wasmFunctions?.engine_series_max_f64 || createWasmFallback("engine_series_max_f64");
export const engine_series_mean_f64 = wasmFunctions?.engine_series_mean_f64 || createWasmFallback("engine_series_mean_f64");
export const engine_series_min_f64 = wasmFunctions?.engine_series_min_f64 || createWasmFallback("engine_series_min_f64");
export const engine_series_ptr_f64 = wasmFunctions?.engine_series_ptr_f64 || createWasmFallback("engine_series_ptr_f64");
export const engine_series_std_f64 = wasmFunctions?.engine_series_std_f64 || createWasmFallback("engine_series_std_f64");
export const engine_series_sum_f64 = wasmFunctions?.engine_series_sum_f64 || createWasmFallback("engine_series_sum_f64");
export const engine_series_to_vec_f64 = wasmFunctions?.engine_series_to_vec_f64 || createWasmFallback("engine_series_to_vec_f64");
export const engine_series_to_vec_i32 = wasmFunctions?.engine_series_to_vec_i32 || createWasmFallback("engine_series_to_vec_i32");
export const engine_sort_indices_f64 = wasmFunctions?.engine_sort_indices_f64 || createWasmFallback("engine_sort_indices_f64");
export const engine_sort_indices_i32 = wasmFunctions?.engine_sort_indices_i32 || createWasmFallback("engine_sort_indices_i32");
export const engine_sort_two_columns_indices_f64 = wasmFunctions?.engine_sort_two_columns_indices_f64 ||
    createWasmFallback("engine_sort_two_columns_indices_f64");
export const engine_sort_two_columns_indices_i32 = wasmFunctions?.engine_sort_two_columns_indices_i32 ||
    createWasmFallback("engine_sort_two_columns_indices_i32");
export const engine_sort_values_f64 = wasmFunctions?.engine_sort_values_f64 || createWasmFallback("engine_sort_values_f64");
export const isin_f64 = wasmFunctions?.isin_f64 || createWasmFallback("isin_f64");
export const isin_i32 = wasmFunctions?.isin_i32 || createWasmFallback("isin_i32");
export const isin_string = wasmFunctions?.isin_string || createWasmFallback("isin_string");
export const engine_memory_usage = wasmFunctions?.engine_memory_usage || createWasmFallback("engine_memory_usage");
export const engine_series_count = wasmFunctions?.engine_series_count || createWasmFallback("engine_series_count");
export const count_non_null_f64 = wasmFunctions?.count_non_null_f64 || createWasmFallback("count_non_null_f64");
