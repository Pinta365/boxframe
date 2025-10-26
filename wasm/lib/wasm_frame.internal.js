// @generated file from wasmbuild -- do not edit
// @ts-nocheck: generated
// deno-lint-ignore-file
// deno-fmt-ignore-file

let wasm;
export function __wbg_set_wasm(val) {
    wasm = val;
}

let WASM_VECTOR_LEN = 0;

let cachedUint8ArrayMemory0 = null;

function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

const lTextEncoder = typeof TextEncoder === "undefined" ? (0, module.require)("util").TextEncoder : TextEncoder;

const cachedTextEncoder = new lTextEncoder("utf-8");

const encodeString = typeof cachedTextEncoder.encodeInto === "function"
    ? function (arg, view) {
        return cachedTextEncoder.encodeInto(arg, view);
    }
    : function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length,
        };
    };

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }

    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = encodeString(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

let cachedDataViewMemory0 = null;

function getDataViewMemory0() {
    if (
        cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true ||
        (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)
    ) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

const lTextDecoder = typeof TextDecoder === "undefined" ? (0, module.require)("util").TextDecoder : TextDecoder;

let cachedTextDecoder = new lTextDecoder("utf-8", { ignoreBOM: true, fatal: true });

cachedTextDecoder.decode();

const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new lTextDecoder("utf-8", { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedFloat64ArrayMemory0 = null;

function getFloat64ArrayMemory0() {
    if (cachedFloat64ArrayMemory0 === null || cachedFloat64ArrayMemory0.byteLength === 0) {
        cachedFloat64ArrayMemory0 = new Float64Array(wasm.memory.buffer);
    }
    return cachedFloat64ArrayMemory0;
}

function passArrayF64ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 8, 8) >>> 0;
    getFloat64ArrayMemory0().set(arg, ptr / 8);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}
/**
 * @param {Float64Array} data
 * @returns {number}
 */
export function engine_create_series_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_create_series_f64(ptr0, len0);
    return ret >>> 0;
}

let cachedUint32ArrayMemory0 = null;

function getUint32ArrayMemory0() {
    if (cachedUint32ArrayMemory0 === null || cachedUint32ArrayMemory0.byteLength === 0) {
        cachedUint32ArrayMemory0 = new Uint32Array(wasm.memory.buffer);
    }
    return cachedUint32ArrayMemory0;
}

function passArray32ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 4, 4) >>> 0;
    getUint32ArrayMemory0().set(arg, ptr / 4);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}
/**
 * @param {Int32Array} data
 * @returns {number}
 */
export function engine_create_series_i32(data) {
    const ptr0 = passArray32ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_create_series_i32(ptr0, len0);
    return ret >>> 0;
}

/**
 * @param {number} series_id
 */
export function engine_free_series(series_id) {
    wasm.engine_free_series(series_id);
}

/**
 * @param {number} series_id
 */
export function engine_free_series_i32(series_id) {
    wasm.engine_free_series_i32(series_id);
}

export function engine_flush() {
    wasm.engine_flush();
}

/**
 * @returns {number}
 */
export function engine_memory_usage() {
    const ret = wasm.engine_memory_usage();
    return ret >>> 0;
}

/**
 * @returns {number}
 */
export function engine_series_count() {
    const ret = wasm.engine_series_count();
    return ret >>> 0;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_ptr_f64(series_id) {
    const ret = wasm.engine_series_ptr_f64(series_id);
    return ret >>> 0;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_len_f64(series_id) {
    const ret = wasm.engine_series_len_f64(series_id);
    return ret >>> 0;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_ptr_i32(series_id) {
    const ret = wasm.engine_series_ptr_i32(series_id);
    return ret >>> 0;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_len_i32(series_id) {
    const ret = wasm.engine_series_len_i32(series_id);
    return ret >>> 0;
}

function getArrayF64FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getFloat64ArrayMemory0().subarray(ptr / 8, ptr / 8 + len);
}
/**
 * @param {number} series_id
 * @returns {Float64Array}
 */
export function engine_series_to_vec_f64(series_id) {
    const ret = wasm.engine_series_to_vec_f64(series_id);
    var v1 = getArrayF64FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 8, 8);
    return v1;
}

let cachedInt32ArrayMemory0 = null;

function getInt32ArrayMemory0() {
    if (cachedInt32ArrayMemory0 === null || cachedInt32ArrayMemory0.byteLength === 0) {
        cachedInt32ArrayMemory0 = new Int32Array(wasm.memory.buffer);
    }
    return cachedInt32ArrayMemory0;
}

function getArrayI32FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getInt32ArrayMemory0().subarray(ptr / 4, ptr / 4 + len);
}
/**
 * @param {number} series_id
 * @returns {Int32Array}
 */
export function engine_series_to_vec_i32(series_id) {
    const ret = wasm.engine_series_to_vec_i32(series_id);
    var v1 = getArrayI32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v1;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_sum_f64(series_id) {
    const ret = wasm.engine_series_sum_f64(series_id);
    return ret;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_mean_f64(series_id) {
    const ret = wasm.engine_series_mean_f64(series_id);
    return ret;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_std_f64(series_id) {
    const ret = wasm.engine_series_std_f64(series_id);
    return ret;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_min_f64(series_id) {
    const ret = wasm.engine_series_min_f64(series_id);
    return ret;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_max_f64(series_id) {
    const ret = wasm.engine_series_max_f64(series_id);
    return ret;
}

/**
 * @param {number} series_id
 * @returns {number}
 */
export function engine_series_count_f64(series_id) {
    const ret = wasm.engine_series_count_f64(series_id);
    return ret >>> 0;
}

/**
 * GroupBy sum using an existing registered f64 series and JSON keys
 * Returns a new series_id for the aggregated result (values sorted by key)
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_sum_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_sum_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy mean using an existing registered f64 series and JSON keys
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_mean_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_mean_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy count (non-null) using an existing registered f64 series and JSON keys
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_count_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_count_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy min using an existing registered f64 series and JSON keys
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_min_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_min_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy max using an existing registered f64 series and JSON keys
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_max_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_max_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy std using an existing registered f64 series and JSON keys (sample std, N-1)
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_std_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_std_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * GroupBy var using an existing registered f64 series and JSON keys (sample var, N-1)
 * @param {number} series_id
 * @param {string} group_keys_json
 * @returns {number}
 */
export function engine_groupby_var_f64(series_id, group_keys_json) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_var_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

function getArrayU32FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint32ArrayMemory0().subarray(ptr / 4, ptr / 4 + len);
}
/**
 * Batch multi-aggregation for groupby on f64 series.
 * agg_mask bit layout (LSB -> MSB):
 * 1=sum, 2=mean, 4=count, 8=min, 16=max, 32=std, 64=var
 * Returns array of series ids in the above order for bits that are set.
 * @param {number} series_id
 * @param {string} group_keys_json
 * @param {number} agg_mask
 * @returns {Uint32Array}
 */
export function engine_groupby_multi_f64(series_id, group_keys_json, agg_mask) {
    const ptr0 = passStringToWasm0(group_keys_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_groupby_multi_f64(series_id, ptr0, len0, agg_mask);
    var v2 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v2;
}

/**
 * Sort values (float64) ascending/descending, nulls last flag applies to NaN
 * @param {number} series_id
 * @param {number} ascending
 * @param {number} nulls_last
 * @returns {number}
 */
export function engine_sort_values_f64(series_id, ascending, nulls_last) {
    const ret = wasm.engine_sort_values_f64(series_id, ascending, nulls_last);
    return ret >>> 0;
}

/**
 * Return sort indices (float64) for a registered series (no materialization)
 * @param {number} series_id
 * @param {number} ascending
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function engine_sort_indices_f64(series_id, ascending, nulls_last) {
    const ret = wasm.engine_sort_indices_f64(series_id, ascending, nulls_last);
    var v1 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v1;
}

/**
 * Return sort indices by two registered f64 series (provided as two series ids)
 * @param {number} series1_id
 * @param {number} series2_id
 * @param {number} asc1
 * @param {number} asc2
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function engine_sort_two_columns_indices_f64(series1_id, series2_id, asc1, asc2, nulls_last) {
    const ret = wasm.engine_sort_two_columns_indices_f64(series1_id, series2_id, asc1, asc2, nulls_last);
    var v1 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v1;
}

/**
 * Return sort indices (int32) for a registered i32 series
 * @param {number} series_id
 * @param {number} ascending
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function engine_sort_indices_i32(series_id, ascending, nulls_last) {
    const ret = wasm.engine_sort_indices_i32(series_id, ascending, nulls_last);
    var v1 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v1;
}

/**
 * Return sort indices by two registered i32 series
 * @param {number} series1_id
 * @param {number} series2_id
 * @param {number} asc1
 * @param {number} asc2
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function engine_sort_two_columns_indices_i32(series1_id, series2_id, asc1, asc2, nulls_last) {
    const ret = wasm.engine_sort_two_columns_indices_i32(series1_id, series2_id, asc1, asc2, nulls_last);
    var v1 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v1;
}

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
 * @param {Float64Array} col1
 * @param {Float64Array} col2
 * @param {number} asc1
 * @param {number} asc2
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function sort_two_columns_f64(col1, col2, asc1, asc2, nulls_last) {
    const ptr0 = passArrayF64ToWasm0(col1, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArrayF64ToWasm0(col2, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.sort_two_columns_f64(ptr0, len0, ptr1, len1, asc1, asc2, nulls_last);
    var v3 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v3;
}

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
 * @param {Int32Array} col1
 * @param {Int32Array} col2
 * @param {number} asc1
 * @param {number} asc2
 * @param {number} nulls_last
 * @returns {Uint32Array}
 */
export function sort_two_columns_i32(col1, col2, asc1, asc2, nulls_last) {
    const ptr0 = passArray32ToWasm0(col1, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray32ToWasm0(col2, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.sort_two_columns_i32(ptr0, len0, ptr1, len1, asc1, asc2, nulls_last);
    var v3 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v3;
}

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
 * @param {Float64Array} data
 * @param {boolean} ascending
 * @param {boolean} nulls_last
 * @returns {Uint32Array}
 */
export function sort_single_column_f64(data, ascending, nulls_last) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.sort_single_column_f64(ptr0, len0, ascending, nulls_last);
    var v2 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v2;
}

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
 * @param {Int32Array} data
 * @param {boolean} ascending
 * @param {boolean} nulls_last
 * @returns {Uint32Array}
 */
export function sort_single_column_i32(data, ascending, nulls_last) {
    const ptr0 = passArray32ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.sort_single_column_i32(ptr0, len0, ascending, nulls_last);
    var v2 = getArrayU32FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
    return v2;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}
/**
 * Filter float64 series using a boolean mask (1=true, 0=false)
 * @param {number} series_id
 * @param {Uint8Array} mask
 * @returns {number}
 */
export function engine_filter_f64(series_id, mask) {
    const ptr0 = passArray8ToWasm0(mask, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.engine_filter_f64(series_id, ptr0, len0);
    return ret >>> 0;
}

/**
 * High-performance filtering with boolean mask (using u8 array for WASM compatibility)
 * @param {Float64Array} data
 * @param {Uint8Array} mask
 * @returns {Float64Array}
 */
export function filter_f64(data, mask) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(mask, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.filter_f64(ptr0, len0, ptr1, len1);
    var v3 = getArrayF64FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 8, 8);
    return v3;
}

/**
 * High-performance vectorized sum
 * @param {Float64Array} data
 * @returns {number}
 */
export function sum_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.sum_f64(ptr0, len0);
    return ret;
}

/**
 * High-performance vectorized mean
 * @param {Float64Array} data
 * @returns {number}
 */
export function mean_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.mean_f64(ptr0, len0);
    return ret;
}

/**
 * High-performance vectorized standard deviation (sample)
 * @param {Float64Array} data
 * @returns {number}
 */
export function std_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.std_f64(ptr0, len0);
    return ret;
}

/**
 * High-performance vectorized min
 * @param {Float64Array} data
 * @returns {number}
 */
export function min_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.min_f64(ptr0, len0);
    return ret;
}

/**
 * High-performance vectorized max
 * @param {Float64Array} data
 * @returns {number}
 */
export function max_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.max_f64(ptr0, len0);
    return ret;
}

/**
 * Count non-null values
 * @param {Float64Array} data
 * @returns {number}
 */
export function count_non_null_f64(data) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.count_non_null_f64(ptr0, len0);
    return ret >>> 0;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}
/**
 * Check if values in an array are members of a given set (i32)
 *
 * # Arguments
 * * `data` - Array of i32 values to check
 * * `values` - Array of i32 values to check membership against
 *
 * # Returns
 * * Array of u8 values (0 = false, 1 = true) indicating membership
 * @param {Int32Array} data
 * @param {Int32Array} values
 * @returns {Uint8Array}
 */
export function isin_i32(data, values) {
    const ptr0 = passArray32ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray32ToWasm0(values, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.isin_i32(ptr0, len0, ptr1, len1);
    var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v3;
}

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
 * @param {Float64Array} data
 * @param {Float64Array} values
 * @param {number} tolerance
 * @returns {Uint8Array}
 */
export function isin_f64(data, values, tolerance) {
    const ptr0 = passArrayF64ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArrayF64ToWasm0(values, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.isin_f64(ptr0, len0, ptr1, len1, tolerance);
    var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v3;
}

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_export_2.set(idx, obj);
    return idx;
}

function passArrayJsValueToWasm0(array, malloc) {
    const ptr = malloc(array.length * 4, 4) >>> 0;
    for (let i = 0; i < array.length; i++) {
        const add = addToExternrefTable0(array[i]);
        getDataViewMemory0().setUint32(ptr + 4 * i, add, true);
    }
    WASM_VECTOR_LEN = array.length;
    return ptr;
}
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
 * @param {string[]} data
 * @param {string[]} values
 * @returns {Uint8Array}
 */
export function isin_string(data, values) {
    const ptr0 = passArrayJsValueToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArrayJsValueToWasm0(values, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.isin_string(ptr0, len0, ptr1, len1);
    var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v3;
}

export function __wbg_wbindgenstringget_c45e0c672ada3c64(arg0, arg1) {
    const obj = arg1;
    const ret = typeof obj === "string" ? obj : undefined;
    var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    var len1 = WASM_VECTOR_LEN;
    getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
    getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
}

export function __wbg_wbindgenthrow_681185b504fabc8e(arg0, arg1) {
    throw new Error(getStringFromWasm0(arg0, arg1));
}

export function __wbindgen_init_externref_table() {
    const table = wasm.__wbindgen_export_2;
    const offset = table.grow(4);
    table.set(0, undefined);
    table.set(offset + 0, undefined);
    table.set(offset + 1, null);
    table.set(offset + 2, true);
    table.set(offset + 3, false);
}
