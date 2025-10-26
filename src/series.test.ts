/**
 * Tests for Series class
 *
 * Tests all Series operations including Rust WASM optimizations
 */

import { assertEquals, assertThrows } from "@std/assert";
import { Series } from "./series.ts";

function generateNumericData(size: number): number[] {
    return Array.from({ length: size }, (_, _i) => Math.random() * 1000);
}

function generateIntData(size: number): number[] {
    return Array.from({ length: size }, (_, _i) => Math.floor(Math.random() * 100));
}

Deno.test("Series - Basic creation and properties", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data, { name: "test_series" });

    assertEquals(series.length, 5);
    assertEquals(series.name, "test_series");
    assertEquals(series.dtype, "int32");
    assertEquals(series.shape, [5]);
    assertEquals(series.values, data);
});

Deno.test("Series - Data type inference", () => {
    const intSeries = new Series([1, 2, 3]);
    assertEquals(intSeries.dtype, "int32");

    const floatSeries = new Series([1.5, 2.7, 3.9]);
    assertEquals(floatSeries.dtype, "float64");

    const stringSeries = new Series(["a", "b", "c"]);
    assertEquals(stringSeries.dtype, "string");

    const mixedSeries = new Series([1, "a", 2.5]);
    assertEquals(mixedSeries.dtype, "string");
});

Deno.test("Series - Index handling", () => {
    const data = [10, 20, 30];
    const customIndex = ["a", "b", "c"];
    const series = new Series(data, { index: customIndex });

    assertEquals(series.index, customIndex);
    assertEquals(series.get("a"), 10);
    assertEquals(series.get("b"), 20);
    assertEquals(series.get("c"), 30);
    assertEquals(series.get("d"), undefined);
});

Deno.test("Series - Index validation", () => {
    const data = [1, 2, 3];
    const invalidIndex = ["a", "b"];

    assertThrows(
        () => new Series(data, { index: invalidIndex }),
        Error,
        "Index length (2) must match data length (3)",
    );
});

Deno.test("Series - iloc and loc methods", () => {
    const data = [10, 20, 30, 40, 50];
    const index = ["a", "b", "c", "d", "e"];
    const series = new Series(data, { index });

    assertEquals(series.iloc(0), 10);
    assertEquals(series.iloc(2), 30);
    assertEquals(series.iloc(4), 50);
    assertEquals(series.iloc(5), undefined);

    const selected = series.loc(["a", "c", "e"]);
    assertEquals(selected.values, [10, 30, 50]);
    assertEquals(selected.index, ["a", "c", "e"]);
});

Deno.test("Series - ilocRange method", () => {
    const data = [10, 20, 30, 40, 50];
    const series = new Series(data);

    const selected = series.ilocRange([0, 2, 4]);
    assertEquals(selected.values, [10, 30, 50]);
    assertEquals(selected.index, [0, 2, 4]);
});

Deno.test("Series - filter method", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data);

    const mask = [true, false, true, false, true];
    const filtered = series.filter(mask);

    assertEquals(filtered.values, [1, 3, 5]);
    assertEquals(filtered.length, 3);
});

Deno.test("Series - filter with mask length validation", () => {
    const data = [1, 2, 3];
    const series = new Series(data);
    const invalidMask = [true, false];

    assertThrows(
        () => series.filter(invalidMask),
        Error,
        "Mask length (2) must match Series length (3)",
    );
});

Deno.test("Series - drop method", () => {
    const data = [1, 2, 3, 4, 5];
    const index = ["a", "b", "c", "d", "e"];
    const series = new Series(data, { index });

    const dropped = series.drop(["b", "d"]);
    assertEquals(dropped.values, [1, 3, 5]);
    assertEquals(dropped.index, ["a", "c", "e"]);
});

Deno.test("Series - isin method with int32 data", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data, { dtype: "int32" });

    const result = series.isin([2, 4, 6]);
    assertEquals(result.values, [false, true, false, true, false]);
    assertEquals(result.dtype, "bool");
});

Deno.test("Series - isin method with float64 data", () => {
    const data = [1.1, 2.2, 3.3, 4.4, 5.5];
    const series = new Series(data, { dtype: "float64" });

    const result = series.isin([2.2, 4.4, 6.6]);
    assertEquals(result.values, [false, true, false, true, false]);
    assertEquals(result.dtype, "bool");
});

Deno.test("Series - isin method with string data", () => {
    const data = ["a", "b", "c", "d", "e"];
    const series = new Series(data, { dtype: "string" });

    const result = series.isin(["b", "d", "f"]);
    assertEquals(result.values, [false, true, false, true, false]);
    assertEquals(result.dtype, "bool");
});

Deno.test("Series - isnull and notnull methods", () => {
    const data = [1, null, 3, null, 5];
    const series = new Series(data);

    const isnull = series.isnull();
    assertEquals(isnull.values, [false, true, false, true, false]);

    const notnull = series.notnull();
    assertEquals(notnull.values, [true, false, true, false, true]);
});

Deno.test("Series - dropna method", () => {
    const data = [1, null, 3, null, 5];
    const series = new Series(data);

    const cleaned = series.dropna();
    assertEquals(cleaned.values, [1, 3, 5]);
    assertEquals(cleaned.length, 3);
});

Deno.test("Series - fillna method", () => {
    const data = [1, null, 3, null, 5];
    const series = new Series(data);

    const filled = series.fillna(0);
    assertEquals(filled.values, [1, 0, 3, 0, 5]);
});

Deno.test("Series - unique method", () => {
    const data = [1, 2, 2, 3, 3, 3];
    const series = new Series(data);

    const unique = series.unique();
    assertEquals(unique.sort(), [1, 2, 3]);
});

Deno.test("Series - valueCounts method", () => {
    const data = [1, 2, 2, 3, 3, 3];
    const series = new Series(data);

    const counts = series.valueCounts();
    assertEquals(counts.values.sort(), [1, 2, 3]);
    assertEquals(counts.index.sort(), ["1", "2", "3"]);
});

Deno.test("Series - nunique method", () => {
    const data = [1, 2, 2, 3, 3, 3, 4];
    const series = new Series(data);

    assertEquals(series.nunique(), 4);
    assertEquals(series.nunique(false), 4);
});

Deno.test("Series - nunique with nulls", () => {
    const data = [1, 2, null, 2, 3, null, 4];
    const series = new Series(data);

    assertEquals(series.nunique(), 4);
    assertEquals(series.nunique(false), 5);
});

Deno.test("Series - isUnique method", () => {
    const data = [1, 2, 2, 3, 4, 4, 5];
    const series = new Series(data);

    assertEquals(series.isUnique(), false);
});

Deno.test("Series - isUnique with all unique values", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data);

    assertEquals(series.isUnique(), true);
});

Deno.test("Series - isUnique with all duplicates", () => {
    const data = [1, 1, 1, 1, 1];
    const series = new Series(data);

    assertEquals(series.isUnique(), false);
});

Deno.test("Series - isUnique with nulls", () => {
    const data = [1, 2, 3, null, 4];
    const series = new Series(data);

    assertEquals(series.isUnique(), true);
});

Deno.test("Series - isUnique with duplicate nulls", () => {
    const data = [1, 2, null, 3, null];
    const series = new Series(data);

    assertEquals(series.isUnique(), false);
});

Deno.test("Series - sum method", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data, { dtype: "float64" });

    assertEquals(series.sum(), 15);
});

Deno.test("Series - mean method", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data, { dtype: "float64" });

    assertEquals(series.mean(), 3);
});

Deno.test("Series - std method", () => {
    const data = [1, 2, 3, 4, 5];
    const series = new Series(data, { dtype: "float64" });

    const std = series.std();
    assertEquals(Math.round(std * 100) / 100, 1.58);
});

Deno.test("Series - min and max methods", () => {
    const data = [5, 2, 8, 1, 9];
    const series = new Series(data, { dtype: "float64" });

    assertEquals(series.min(), 1);
    assertEquals(series.max(), 9);
});

Deno.test("Series - count method", () => {
    const data = [1, null, 3, null, 5];
    const series = new Series(data, { dtype: "float64" });

    assertEquals(series.count(), 3);
});

Deno.test("Series - sortValues ascending", () => {
    const data = [5, 2, 8, 1, 9];
    const series = new Series(data, { dtype: "float64" });

    const sorted = series.sortValues(true);
    assertEquals(sorted.values, [1, 2, 5, 8, 9]);
});

Deno.test("Series - sortValues descending", () => {
    const data = [5, 2, 8, 1, 9];
    const series = new Series(data, { dtype: "float64" });

    const sorted = series.sortValues(false);
    assertEquals(sorted.values, [9, 8, 5, 2, 1]);
});

Deno.test("Series - sortValues with nulls", () => {
    const data = [5, null, 2, null, 8];
    const series = new Series(data, { dtype: "float64" });

    const sorted = series.sortValues(true);
    assertEquals(sorted.values, [2, 5, 8, null, null]);
});

Deno.test("Series - sortIndex method", () => {
    const data = [10, 20, 30];
    const index = ["c", "a", "b"];
    const series = new Series(data, { index });

    const sorted = series.sortIndex(true);
    assertEquals(sorted.index, ["a", "b", "c"]);
    assertEquals(sorted.values, [20, 30, 10]);
});

Deno.test("Series - rename method", () => {
    const data = [1, 2, 3];
    const series = new Series(data, { name: "old_name" });

    const renamed = series.rename("new_name");
    assertEquals(renamed.name, "new_name");
    assertEquals(renamed.values, data);
});

Deno.test("Series - head and tail methods", () => {
    const data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const series = new Series(data);

    const head = series.head(3);
    assertEquals(head.values, [1, 2, 3]);

    const tail = series.tail(3);
    assertEquals(tail.values, [8, 9, 10]);
});

Deno.test("Series - resetIndex method", () => {
    const data = [10, 20, 30];
    const customIndex = ["a", "b", "c"];
    const series = new Series(data, { index: customIndex });

    const reset = series.resetIndex();
    assertEquals(reset.index, [0, 1, 2]);
    assertEquals(reset.values, data);
});

Deno.test("Series - toString method", () => {
    const data = [1, 2, 3];
    const series = new Series(data, { name: "test" });

    const str = series.toString();
    assertEquals(str.includes("Series: test"), true);
    assertEquals(str.includes("dtype: int32"), true);
    assertEquals(str.includes("length: 3"), true);
});

Deno.test("Series - toJSON method", () => {
    const data = [1, 2, 3];
    const index = ["a", "b", "c"];
    const series = new Series(data, { name: "test", index });

    const json = series.toJSON();
    assertEquals(json.name, "test");
    assertEquals(json.dtype, "int32");
    assertEquals(json.index, index);
    assertEquals(json.data, data);
});

Deno.test("Series - Performance: isin with large dataset", () => {
    const size = 10000;
    const data = generateIntData(size);
    const series = new Series(data, { dtype: "int32" });
    const values = [1, 2, 3, 4, 5];

    const start = performance.now();
    const result = series.isin(values);
    const end = performance.now();

    assertEquals(result.length, size);
    assertEquals(result.dtype, "bool");
    console.log(`isin performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});

Deno.test("Series - Performance: sorting with large dataset", () => {
    const size = 10000;
    const data = generateNumericData(size);
    const series = new Series(data, { dtype: "float64" });

    const start = performance.now();
    const sorted = series.sortValues(true);
    const end = performance.now();

    assertEquals(sorted.length, size);
    assertEquals(sorted.dtype, "float64");
    console.log(`sorting performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});

// Test WASM fallback behavior when WASM is disabled
Deno.test("Series - WASM fallback behavior when disabled", () => {
    // Temporarily disable WASM
    const originalValue = globalThis.DF_USE_WASM_ENGINE;
    globalThis.DF_USE_WASM_ENGINE = false;

    try {
        // Test that Series operations still work with JavaScript fallbacks
        const data = [1, 2, 3, 4, 5];
        const series = new Series(data, { dtype: "float64" });

        // Test basic operations that should fall back to JavaScript
        assertEquals(series.sum(), 15);
        assertEquals(series.mean(), 3);
        assertEquals(series.min(), 1);
        assertEquals(series.max(), 5);
        assertEquals(series.count(), 5);

        // Test filtering with JavaScript fallback
        const mask = [true, false, true, false, true];
        const filtered = series.filter(mask);
        assertEquals(filtered.values, [1, 3, 5]);

        // Test isin with JavaScript fallback
        const isinResult = series.isin([2, 4]);
        assertEquals(isinResult.values, [false, true, false, true, false]);

        // Test sorting with JavaScript fallback
        const unsorted = new Series([3, 1, 4, 1, 5], { dtype: "float64" });
        const sorted = unsorted.sortValues(true);
        assertEquals(sorted.values, [1, 1, 3, 4, 5]);

        // Test with int32 data
        const intSeries = new Series([10, 20, 30], { dtype: "int32" });
        const intSorted = intSeries.sortValues(true);
        assertEquals(intSorted.values, [10, 20, 30]);

        // Test string isin fallback
        const stringSeries = new Series(["apple", "banana", "cherry"] as string[]);
        const searchValues = ["banana", "date"];
        const stringIsin = stringSeries.isin(searchValues);
        assertEquals(stringIsin.values, [false, true, false]);
    } finally {
        // Restore original value
        globalThis.DF_USE_WASM_ENGINE = originalValue;
    }
});

// Test WASM fallback with error handling
Deno.test("Series - WASM fallback error handling", () => {
    // Temporarily disable WASM
    const originalValue = globalThis.DF_USE_WASM_ENGINE;
    globalThis.DF_USE_WASM_ENGINE = false;

    try {
        const data = [1, 2, 3, 4, 5];
        const series = new Series(data, { dtype: "float64" });

        // Test that operations handle edge cases gracefully
        const emptySeries = new Series([], { dtype: "float64" });
        assertEquals(emptySeries.sum(), 0);
        assertEquals(emptySeries.mean(), NaN);
        assertEquals(emptySeries.min(), NaN);
        assertEquals(emptySeries.max(), NaN);
        assertEquals(emptySeries.count(), 0);

        // Test with null values
        const nullSeries = new Series([1, null, 3, null, 5], { dtype: "float64" });
        assertEquals(nullSeries.count(), 3);
        assertEquals(nullSeries.sum(), 9);
        assertEquals(nullSeries.mean(), 3);

        // Test filtering with null values
        const nullMask = [true, false, true, false, true];
        const nullFiltered = nullSeries.filter(nullMask);
        assertEquals(nullFiltered.values, [1, 3, 5]);
    } finally {
        // Restore original value
        globalThis.DF_USE_WASM_ENGINE = originalValue;
    }
});

// Test WASM fallback performance comparison
Deno.test("Series - WASM fallback performance", () => {
    // Temporarily disable WASM
    const originalValue = globalThis.DF_USE_WASM_ENGINE;
    globalThis.DF_USE_WASM_ENGINE = false;

    try {
        const size = 10000;
        const data = Array.from({ length: size }, (_, i) => Math.random() * 1000);
        const series = new Series(data, { dtype: "float64" });

        // Test JavaScript fallback performance
        const start = performance.now();
        const sum = series.sum();
        const mean = series.mean();
        const sorted = series.sortValues(true);
        const end = performance.now();

        assertEquals(typeof sum, "number");
        assertEquals(typeof mean, "number");
        assertEquals(sorted.length, size);
        console.log(`JavaScript fallback performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
    } finally {
        // Restore original value
        globalThis.DF_USE_WASM_ENGINE = originalValue;
    }
});
