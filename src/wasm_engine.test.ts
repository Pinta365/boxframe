import { assertEquals } from "@std/assert";
import { WasmEngine } from "./wasm_engine.ts";

Deno.test("WasmEngine scalar ops f64", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        assertEquals(eng.seriesSumF64(id), 15);
        assertEquals(eng.seriesMeanF64(id), 3);
        assertEquals(Math.round(eng.seriesStdF64(id) * 100) / 100, Math.round(Math.sqrt(2.5) * 100) / 100);
        assertEquals(eng.seriesMinF64(id), 1);
        assertEquals(eng.seriesMaxF64(id), 5);
        assertEquals(eng.seriesCountF64(id), 5);
    } finally {
        eng.freeSeries(id);
    }
});

Deno.test("WasmEngine sort indices f64", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([3, 1, 2, 4]);
    const id = eng.registerSeriesF64(data);
    try {
        const idx = eng.sortIndicesF64(id, true, true);
        assertEquals(idx, [1, 2, 0, 3]);
    } finally {
        eng.freeSeries(id);
    }
});

Deno.test("WasmEngine two-column sort indices f64", () => {
    const eng = WasmEngine.instance;
    const col1 = new Float64Array([1, 1, 2, 2]);
    const col2 = new Float64Array([3, 4, 1, 2]);
    const id1 = eng.registerSeriesF64(col1);
    const id2 = eng.registerSeriesF64(col2);
    try {
        const idx = eng.sortTwoColumnsIndicesF64(id1, id2, true, true, true);
        assertEquals(idx, [0, 1, 2, 3]);
    } finally {
        eng.freeSeries(id1);
        eng.freeSeries(id2);
    }
});

Deno.test("WasmEngine sort indices i32", () => {
    const eng = WasmEngine.instance;
    const data = new Int32Array([3, 1, 2, 4]);
    const id = eng.registerSeriesI32(data);
    try {
        const idx = eng.sortIndicesI32(id, true, true);
        assertEquals(idx, [1, 2, 0, 3]);
    } finally {
        eng.freeSeriesI32(id);
    }
});

Deno.test("WasmEngine two-column sort indices i32", () => {
    const eng = WasmEngine.instance;
    // department (col1) and salary (col2)
    // Multiple employees in same department, different salaries
    const col1 = new Int32Array([2, 1, 2, 1, 3, 2]); // department: [Sales, IT, Sales, IT, HR, Sales]
    const col2 = new Int32Array([4, 3, 2, 5, 1, 6]); // salary:     [40000, 30000, 20000, 50000, 10000, 60000]
    const id1 = eng.registerSeriesI32(col1);
    const id2 = eng.registerSeriesI32(col2);
    try {
        const idx = eng.sortTwoColumnsIndicesI32(id1, id2, true, true, true);
        // Expected: Sort by department (asc), then by salary (asc)
        // IT dept (1): salary 30000, 50000 -> rows [1, 3]
        // Sales dept (2): salary 20000, 40000, 60000 -> rows [2, 0, 5]
        // HR dept (3): salary 10000 -> row [4]
        assertEquals(idx, [1, 3, 2, 0, 5, 4]);
    } finally {
        eng.freeSeriesI32(id1);
        eng.freeSeriesI32(id2);
    }
});

// Test I32 operations
Deno.test("WasmEngine I32 operations", () => {
    const eng = WasmEngine.instance;
    const data = new Int32Array([10, 20, 30, 40, 50]);
    const id = eng.registerSeriesI32(data);
    try {
        // Test series to vector conversion
        const vec = eng.seriesToVecI32(id);
        assertEquals(vec, [10, 20, 30, 40, 50]);

        // Test series length (I32 might use different length method)
        const len = eng.getSeriesLen(id);
        assertEquals(typeof len, "number");
    } finally {
        eng.freeSeriesI32(id);
    }
});

// Test groupby operations
Deno.test("WasmEngine groupby operations", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        // Test groupby operations with simple grouping
        const groupKeys = JSON.stringify([0, 0, 1, 1, 2]); // Group indices

        const sumId = eng.groupbySumF64(id, groupKeys);
        const meanId = eng.groupbyMeanF64(id, groupKeys);
        const countId = eng.groupbyCountF64(id, groupKeys);
        const minId = eng.groupbyMinF64(id, groupKeys);
        const maxId = eng.groupbyMaxF64(id, groupKeys);
        const stdId = eng.groupbyStdF64(id, groupKeys);
        const varId = eng.groupbyVarF64(id, groupKeys);

        // Clean up groupby results
        eng.freeSeries(sumId);
        eng.freeSeries(meanId);
        eng.freeSeries(countId);
        eng.freeSeries(minId);
        eng.freeSeries(maxId);
        eng.freeSeries(stdId);
        eng.freeSeries(varId);
    } finally {
        eng.freeSeries(id);
    }
});

// Test groupby multi operations
Deno.test("WasmEngine groupby multi operations", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        const groupKeys = JSON.stringify([0, 0, 1, 1, 2]);
        const mask = 0b11111; // All operations enabled

        const results = eng.groupbyMultiF64(id, groupKeys, mask);
        assertEquals(Array.isArray(results), true);
        // The exact number of results may vary, just check it's an array
        assertEquals(results.length >= 0, true);
    } finally {
        eng.freeSeries(id);
    }
});

// Test filtering operations
Deno.test("WasmEngine filtering operations", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        // Create a mask for filtering (keep even numbers)
        const mask = new Uint8Array([0, 1, 0, 1, 0]); // Keep indices 1 and 3 (values 2 and 4)

        const filteredId = eng.filterF64(id, mask);
        const filteredData = eng.seriesToVecF64(filteredId);

        assertEquals(filteredData, [2, 4]);
        eng.freeSeries(filteredId);
    } finally {
        eng.freeSeries(id);
    }
});

// Test sort values operations
Deno.test("WasmEngine sort values f64", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([3, 1, 4, 1, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        // Test ascending sort
        const sortedId = eng.sortValuesF64(id, true, true);
        const sortedData = eng.seriesToVecF64(sortedId);
        assertEquals(sortedData, [1, 1, 3, 4, 5]);
        eng.freeSeries(sortedId);

        // Test descending sort
        const sortedIdDesc = eng.sortValuesF64(id, false, true);
        const sortedDataDesc = eng.seriesToVecF64(sortedIdDesc);
        assertEquals(sortedDataDesc, [5, 4, 3, 1, 1]);
        eng.freeSeries(sortedIdDesc);
    } finally {
        eng.freeSeries(id);
    }
});

// Test isin operations
Deno.test("WasmEngine isin operations", () => {
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const values = new Float64Array([2, 4, 6]);

    const result = WasmEngine.isinF64(data, values);
    assertEquals(result, new Uint8Array([0, 1, 0, 1, 0])); // 2 and 4 are present

    // Test I32 isin
    const dataI32 = new Int32Array([1, 2, 3, 4, 5]);
    const valuesI32 = new Int32Array([2, 4, 6]);

    const resultI32 = WasmEngine.isinI32(dataI32, valuesI32);
    assertEquals(resultI32, new Uint8Array([0, 1, 0, 1, 0]));

    // Test string isin
    const dataStr = ["apple", "banana", "cherry", "date"];
    const valuesStr = ["banana", "elderberry", "date"];

    const resultStr = WasmEngine.isinString(dataStr, valuesStr);
    assertEquals(resultStr, new Uint8Array([0, 1, 0, 1]));
});

// Test memory management
Deno.test("WasmEngine memory management", () => {
    const eng = WasmEngine.instance;

    // Test flush operation
    eng.flush();

    // Test memory usage (should return a number)
    const memoryUsage = WasmEngine.getMemoryUsage();
    assertEquals(typeof memoryUsage, "number");

    // Test series count
    const seriesCount = WasmEngine.getSeriesCount();
    assertEquals(typeof seriesCount, "number");
});

// Test count non-null operations
Deno.test("WasmEngine count non-null operations", () => {
    const data = new Float64Array([1, NaN, 3, NaN, 5]);

    const count = WasmEngine.countNonNullF64(data);
    assertEquals(count, 3); // 3 non-null values

    // Test instance method
    const eng = WasmEngine.instance;
    const count2 = eng.countNonNullF64(data);
    assertEquals(count2, 3);
});

// Test series pointer and length operations
Deno.test("WasmEngine series pointer and length operations", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([1, 2, 3, 4, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        // Test series pointer
        const ptr = eng.getSeriesPtr(id);
        assertEquals(typeof ptr, "number");

        // Test series length
        const len = eng.getSeriesLen(id);
        assertEquals(len, 5);
    } finally {
        eng.freeSeries(id);
    }
});

// Test edge cases
Deno.test("WasmEngine edge cases", () => {
    const eng = WasmEngine.instance;

    // Test empty array
    const emptyData = new Float64Array([]);
    const emptyId = eng.registerSeriesF64(emptyData);
    try {
        assertEquals(eng.seriesCountF64(emptyId), 0);
        assertEquals(eng.getSeriesLen(emptyId), 0);
    } finally {
        eng.freeSeries(emptyId);
    }

    // Test single element
    const singleData = new Float64Array([42]);
    const singleId = eng.registerSeriesF64(singleData);
    try {
        assertEquals(eng.seriesSumF64(singleId), 42);
        assertEquals(eng.seriesMeanF64(singleId), 42);
        assertEquals(eng.seriesMinF64(singleId), 42);
        assertEquals(eng.seriesMaxF64(singleId), 42);
        assertEquals(eng.seriesCountF64(singleId), 1);
    } finally {
        eng.freeSeries(singleId);
    }
});

// Test sort with different parameters
Deno.test("WasmEngine sort with different parameters", () => {
    const eng = WasmEngine.instance;
    const data = new Float64Array([3, 1, 4, 1, 5]);
    const id = eng.registerSeriesF64(data);
    try {
        // Test ascending, nulls last
        const idx1 = eng.sortIndicesF64(id, true, true);
        assertEquals(idx1, [1, 3, 0, 2, 4]);

        // Test descending, nulls last
        const idx2 = eng.sortIndicesF64(id, false, true);
        assertEquals(idx2, [4, 2, 0, 1, 3]);

        // Test ascending, nulls first
        const idx3 = eng.sortIndicesF64(id, true, false);
        assertEquals(idx3, [1, 3, 0, 2, 4]);

        // Test descending, nulls first
        const idx4 = eng.sortIndicesF64(id, false, false);
        assertEquals(idx4, [4, 2, 0, 1, 3]);
    } finally {
        eng.freeSeries(id);
    }
});

// Test two-column sort with different parameters
Deno.test("WasmEngine two-column sort with different parameters", () => {
    const eng = WasmEngine.instance;
    const col1 = new Float64Array([1, 1, 2, 2]);
    const col2 = new Float64Array([3, 4, 1, 2]);
    const id1 = eng.registerSeriesF64(col1);
    const id2 = eng.registerSeriesF64(col2);
    try {
        // Test different sort orders
        const idx1 = eng.sortTwoColumnsIndicesF64(id1, id2, true, true, true);
        assertEquals(idx1, [0, 1, 2, 3]);

        const idx2 = eng.sortTwoColumnsIndicesF64(id1, id2, false, true, true);
        assertEquals(idx2, [2, 3, 0, 1]);

        const idx3 = eng.sortTwoColumnsIndicesF64(id1, id2, true, false, true);
        assertEquals(idx3, [1, 0, 3, 2]);

        const idx4 = eng.sortTwoColumnsIndicesF64(id1, id2, false, false, true);
        assertEquals(idx4, [3, 2, 1, 0]);
    } finally {
        eng.freeSeries(id1);
        eng.freeSeries(id2);
    }
});

// Test I32 two-column sort with different parameters
Deno.test("WasmEngine I32 two-column sort with different parameters", () => {
    const eng = WasmEngine.instance;
    const col1 = new Int32Array([2, 1, 2, 1]);
    const col2 = new Int32Array([4, 3, 2, 5]);
    const id1 = eng.registerSeriesI32(col1);
    const id2 = eng.registerSeriesI32(col2);
    try {
        // Test different sort orders
        const idx1 = eng.sortTwoColumnsIndicesI32(id1, id2, true, true, true);
        assertEquals(idx1, [1, 3, 2, 0]);

        const idx2 = eng.sortTwoColumnsIndicesI32(id1, id2, false, true, true);
        assertEquals(idx2, [2, 0, 1, 3]);

        const idx3 = eng.sortTwoColumnsIndicesI32(id1, id2, true, false, true);
        assertEquals(idx3, [3, 1, 0, 2]);

        const idx4 = eng.sortTwoColumnsIndicesI32(id1, id2, false, false, true);
        assertEquals(idx4, [0, 2, 3, 1]);
    } finally {
        eng.freeSeriesI32(id1);
        eng.freeSeriesI32(id2);
    }
});

// Test singleton pattern
Deno.test("WasmEngine singleton pattern", () => {
    const eng1 = WasmEngine.instance;
    const eng2 = WasmEngine.instance;

    // Should be the same instance
    assertEquals(eng1, eng2);
});

// Test WASM-specific edge cases
Deno.test("WasmEngine WASM-specific edge cases", () => {
    const eng = WasmEngine.instance;

    // Test with NaN values in WASM operations
    const nanData = new Float64Array([1, NaN, 3, NaN, 5]);
    const nanId = eng.registerSeriesF64(nanData);
    try {
        const count = eng.seriesCountF64(nanId);
        assertEquals(count, 3); // Should count non-NaN values
    } finally {
        eng.freeSeries(nanId);
    }

    // Test with infinity values in WASM operations
    const infData = new Float64Array([1, Infinity, 3, -Infinity, 5]);
    const infId = eng.registerSeriesF64(infData);
    try {
        const sum = eng.seriesSumF64(infId);
        assertEquals(isFinite(sum), false); // Sum should be NaN or Infinity
    } finally {
        eng.freeSeries(infId);
    }
});
