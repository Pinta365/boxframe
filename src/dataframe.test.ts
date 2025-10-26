/**
 * Tests for DataFrame class
 *
 * Tests all DataFrame operations including Rust WASM optimizations
 */

import { assert, assertEquals, assertThrows } from "@std/assert";
import { DataFrame } from "./dataframe.ts";
import type { RowData } from "./types.ts";

function generateNumericData(size: number): Record<string, number[]> {
    return {
        id: Array.from({ length: size }, (_, i) => i),
        value: Array.from({ length: size }, () => Math.random() * 1000),
        score: Array.from({ length: size }, () => Math.floor(Math.random() * 100)),
        price: Array.from({ length: size }, () => Math.random() * 1000 + 10),
    };
}

Deno.test("DataFrame - Basic creation from Record", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
        col3: [1.1, 2.2, 3.3],
    };
    const df = new DataFrame(data);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["col1", "col2", "col3"]);
    assertEquals(df.index.length, 3);
});

Deno.test("DataFrame - Basic creation from Map", () => {
    const data = new Map([
        ["col1", [1, 2, 3]],
        ["col2", ["a", "b", "c"]],
        ["col3", [1.1, 2.2, 3.3]],
    ]);
    const df = new DataFrame(data);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["col1", "col2", "col3"]);
});

Deno.test("DataFrame - Custom columns and index", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const customColumns = ["x", "y"];
    const customIndex = ["row1", "row2", "row3"];

    const df = new DataFrame(data, { columns: customColumns, index: customIndex });

    assertEquals(df.columns, customColumns);
    assertEquals(df.index, customIndex);
});

Deno.test("DataFrame - Column length validation", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b"],
    };

    assertThrows(
        () => new DataFrame(data),
        Error,
        "must match index length",
    );
});

Deno.test("DataFrame - getColumns method", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
        col3: [1.1, 2.2, 3.3],
    };
    const df = new DataFrame(data);

    const selected = df.getColumns(["col1", "col3"]);
    assertEquals(selected.columns, ["col1", "col3"]);
    assertEquals(selected.shape, [3, 2]);
});

Deno.test("DataFrame - getColumns with non-existent column", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);

    assertThrows(
        () => df.getColumns(["col1", "nonexistent"]),
        Error,
        "Column 'nonexistent' not found",
    );
});

Deno.test("DataFrame - iloc method", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);

    const row = df.iloc([1]);
    assertEquals(row.values[0], [2, "b"]);
    assertEquals(row.index, [1]);
});

Deno.test("DataFrame - iloc with range", () => {
    const data = {
        col1: [1, 2, 3, 4, 5],
        col2: ["a", "b", "c", "d", "e"],
    };
    const df = new DataFrame(data);

    const rows = df.iloc([1, 3]);
    assertEquals(rows.shape, [2, 2]);
    assertEquals(rows.index, [1, 3]);
});

Deno.test("DataFrame - sortValues single column ascending", () => {
    const data = {
        id: [3, 1, 2],
        value: [30, 10, 20],
    };
    const df = new DataFrame(data);

    const sorted = df.sortValues("value");
    const values = sorted.getColumns(["value"]).data.get("value")?.values as number[];
    assertEquals(values, [10, 20, 30]);
});

Deno.test("DataFrame - sortValues single column descending", () => {
    const data = {
        id: [3, 1, 2],
        value: [30, 10, 20],
    };
    const df = new DataFrame(data);

    const sorted = df.sortValues("value", false);
    const values = sorted.getColumns(["value"]).data.get("value")?.values as number[];
    assertEquals(values, [30, 20, 10]);
});

Deno.test("DataFrame - sortValues two columns", () => {
    const data = {
        category: [1, 1, 2, 2],
        value: [30, 20, 10, 40],
    };
    const df = new DataFrame(data);

    const sorted = df.sortValues(["category", "value"]);
    const categories = sorted.getColumns(["category"]).data.get("category")?.values as number[];
    const values = sorted.getColumns(["value"]).data.get("value")?.values as number[];

    assertEquals(categories, [1, 1, 2, 2]);
    assertEquals(values, [20, 30, 10, 40]);
});

Deno.test("DataFrame - sortValues with non-existent column", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);

    assertThrows(
        () => df.sortValues("nonexistent"),
        Error,
        "Column 'nonexistent' not found",
    );
});

Deno.test("DataFrame - sortValues with nulls", () => {
    const data = {
        value: [3, null, 1, null, 2],
    };
    const df = new DataFrame(data);

    const sorted = df.sortValues("value");
    const values = sorted.getColumns(["value"]).data.get("value")?.values as (number | null)[];
    assertEquals(values, [1, 2, 3, null, null]);
});

Deno.test("DataFrame - sortIndex method", () => {
    const data = {
        col1: [10, 20, 30],
        col2: ["a", "b", "c"],
    };
    const customIndex = [2, 0, 1];
    const df = new DataFrame(data, { index: customIndex });

    const sorted = df.sortIndex();
    assertEquals(sorted.index, [0, 1, 2]);
    assertEquals(sorted.getColumns(["col1"]).data.get("col1")?.values, [20, 30, 10]);
});

Deno.test("DataFrame - filter method", () => {
    const data = {
        col1: [1, 2, 3, 4, 5],
        col2: ["a", "b", "c", "d", "e"],
    };
    const df = new DataFrame(data);

    const mask = [true, false, true, false, true];
    const filtered = df.filter(mask);

    assertEquals(filtered.shape, [3, 2]);
    assertEquals(filtered.index, [0, 2, 4]);
});

Deno.test("DataFrame - filter with mask length validation", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);
    const invalidMask = [true, false];

    assertThrows(
        () => df.filter(invalidMask),
        Error,
        "Mask length (2) must match DataFrame length (3)",
    );
});

Deno.test("DataFrame - drop method", () => {
    const data = {
        col1: [1, 2, 3, 4, 5],
        col2: ["a", "b", "c", "d", "e"],
    };
    const df = new DataFrame(data);

    const dropped = df.drop([1, 3]);
    assertEquals(dropped.shape, [3, 2]);
    assertEquals(dropped.index, [0, 2, 4]);
});

Deno.test("DataFrame - dropColumns method", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
        col3: [1.1, 2.2, 3.3],
    };
    const df = new DataFrame(data);

    const dropped = df.dropColumns(["col2"]);
    assertEquals(dropped.columns, ["col1", "col3"]);
    assertEquals(dropped.shape, [3, 2]);
});

Deno.test("DataFrame - assign method with function", () => {
    const data = {
        col1: [1, 2, 3],
        col2: [10, 20, 30],
    };
    const df = new DataFrame(data);

    const assigned = df.assign({
        col3: (row: RowData) => (row.col1 as number) + (row.col2 as number),
    });

    assertEquals(assigned.columns, ["col1", "col2", "col3"]);
    const col3Values = assigned.getColumns(["col3"]).data.get("col3")?.values as number[];
    assertEquals(col3Values, [11, 22, 33]);
});

Deno.test("DataFrame - assign method with array", () => {
    const data = {
        col1: [1, 2, 3],
        col2: [10, 20, 30],
    };
    const df = new DataFrame(data);

    const assigned = df.assign({
        col3: [100, 200, 300],
    });

    assertEquals(assigned.columns, ["col1", "col2", "col3"]);
    const col3Values = assigned.getColumns(["col3"]).data.get("col3")?.values as number[];
    assertEquals(col3Values, [100, 200, 300]);
});

Deno.test("DataFrame - dropDuplicates method", () => {
    const data = {
        col1: [1, 2, 2, 3, 3, 3],
        col2: ["a", "b", "b", "c", "c", "c"],
    };
    const df = new DataFrame(data);

    const deduplicated = df.dropDuplicates();
    assertEquals(deduplicated.shape, [3, 2]);
});

Deno.test("DataFrame - dropDuplicates with subset", () => {
    const data = {
        col1: [1, 2, 2, 3, 3, 3],
        col2: ["a", "b", "c", "d", "e", "f"],
    };
    const df = new DataFrame(data);

    const deduplicated = df.dropDuplicates(["col1"]);
    assertEquals(deduplicated.shape, [3, 2]);
});

Deno.test("DataFrame - isnull method", () => {
    const data = {
        col1: [1, null, 3],
        col2: ["a", "b", null],
    };
    const df = new DataFrame(data);

    const isnull = df.isnull();
    assertEquals(isnull.shape, [3, 2]);
    assertEquals(isnull.columns, ["col1", "col2"]);
});

Deno.test("DataFrame - notnull method", () => {
    const data = {
        col1: [1, null, 3],
        col2: ["a", "b", null],
    };
    const df = new DataFrame(data);

    const notnull = df.notnull();
    assertEquals(notnull.shape, [3, 2]);
    assertEquals(notnull.columns, ["col1", "col2"]);
});

Deno.test("DataFrame - dropna method", () => {
    const data = {
        col1: [1, null, 3, null, 5],
        col2: ["a", "b", null, "d", "e"],
    };
    const df = new DataFrame(data);

    const cleaned = df.dropna();
    assertEquals(cleaned.shape, [2, 2]);
});

Deno.test("DataFrame - fillna method", () => {
    const data = {
        col1: [1, null, 3],
        col2: ["a", "b", null],
    };
    const df = new DataFrame(data);

    const filled = df.fillna({ col1: 0, col2: "unknown" });
    const col1Values = filled.getColumns(["col1"]).data.get("col1")?.values as (number | null)[];
    const col2Values = filled.getColumns(["col2"]).data.get("col2")?.values as (string | null)[];

    assertEquals(col1Values, [1, 0, 3]);
    assertEquals(col2Values, ["a", "b", "unknown"]);
});

Deno.test("DataFrame - head and tail methods", () => {
    const data = {
        col1: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        col2: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    };
    const df = new DataFrame(data);

    const head = df.head(3);
    assertEquals(head.shape, [3, 2]);
    assertEquals(head.index, [0, 1, 2]);

    const tail = df.tail(3);
    assertEquals(tail.shape, [3, 2]);
    assertEquals(tail.index, [7, 8, 9]);
});

Deno.test("DataFrame - resetIndex method", () => {
    const data = {
        col1: [10, 20, 30],
        col2: ["a", "b", "c"],
    };
    const customIndex = ["row1", "row2", "row3"];
    const df = new DataFrame(data, { index: customIndex });

    const reset = df.resetIndex();
    assertEquals(reset.index, [0, 1, 2]);
    assertEquals(reset.columns, ["col1", "col2"]);
});

Deno.test("DataFrame - toString method", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);

    const str = df.toString();
    assertEquals(str.includes("DataFrame"), true);
    assertEquals(str.includes("shape: [3, 2]"), true);
    assertEquals(str.includes("col1"), true);
    assertEquals(str.includes("col2"), true);
});

Deno.test("DataFrame - toJSON method", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };
    const df = new DataFrame(data);

    const json = df.toJSON();
    assertEquals(json.columns, ["col1", "col2"]);
    assertEquals(json.index.length, 3);
    assertEquals(Object.keys(json.data).length, 2);
});

Deno.test("DataFrame - query method: simple numeric condition", () => {
    const data = {
        name: ["Alice", "Bob", "Charlie", "Diana"],
        age: [25, 30, 35, 28],
        salary: [50000, 60000, 70000, 55000],
    };
    const df = new DataFrame(data);

    const result = df.query("age > 28");

    assertEquals(result.shape, [2, 3]);
    assertEquals(result.index, [1, 2]);
    assertEquals(result.get("name").values, ["Bob", "Charlie"]);
    assertEquals(result.get("age").values, [30, 35]);
});

Deno.test("DataFrame - query method: string condition", () => {
    const data = {
        name: ["Alice", "Bob", "Charlie", "Diana"],
        age: [25, 30, 35, 28],
        salary: [50000, 60000, 70000, 55000],
    };
    const df = new DataFrame(data);

    const result = df.query('name == "Alice"');

    assertEquals(result.shape, [1, 3]);
    assertEquals(result.index, [0]);
    assertEquals(result.get("name").values, ["Alice"]);
});

Deno.test("DataFrame - query method: AND condition", () => {
    const data = {
        name: ["Alice", "Bob", "Charlie", "Diana"],
        age: [25, 30, 35, 28],
        salary: [50000, 60000, 70000, 55000],
    };
    const df = new DataFrame(data);

    const result = df.query("age > 25 and salary < 65000");

    assertEquals(result.shape, [2, 3]);
    assertEquals(result.index, [1, 3]);
    assertEquals(result.get("name").values, ["Bob", "Diana"]);
});

Deno.test("DataFrame - query method: OR condition", () => {
    const data = {
        name: ["Alice", "Bob", "Charlie", "Diana"],
        age: [25, 30, 35, 28],
        salary: [50000, 60000, 70000, 55000],
    };
    const df = new DataFrame(data);

    const result = df.query("age < 26 or salary > 65000");

    assertEquals(result.shape, [2, 3]);
    assertEquals(result.index, [0, 2]);
    assertEquals(result.get("name").values, ["Alice", "Charlie"]);
});

Deno.test("DataFrame - query method: complex nested condition", () => {
    const data = {
        name: ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        age: [25, 30, 35, 28, 32],
        salary: [50000, 60000, 70000, 55000, 65000],
        department: ["IT", "HR", "IT", "Finance", "IT"],
    };
    const df = new DataFrame(data);

    const result = df.query("(age > 25 and salary < 65000) or department == 'IT'");

    assertEquals(result.shape, [3, 4]);
    assertEquals(result.index, [0, 2, 4]);
    assertEquals(result.get("name").values, ["Alice", "Charlie", "Eve"]);
});

Deno.test("DataFrame - query method: different operators", () => {
    const data = {
        age: [20, 25, 30, 35, 40],
        score: [85, 90, 95, 88, 92],
    };
    const df = new DataFrame(data);

    const geResult = df.query("age >= 30");
    assertEquals(geResult.shape, [3, 2]);
    assertEquals(geResult.index, [2, 3, 4]);

    const leResult = df.query("score <= 90");
    assertEquals(leResult.shape, [3, 2]);
    assertEquals(leResult.index, [0, 1, 3]);

    const neResult = df.query("age != 30");
    assertEquals(neResult.shape, [4, 2]);
    assertEquals(neResult.index, [0, 1, 3, 4]);
});

Deno.test("DataFrame - query method: error handling", () => {
    const data = {
        age: [25, 30, 35],
        salary: [50000, 60000, 70000],
    };
    const df = new DataFrame(data);

    try {
        df.query("invalid_column > 25");
        assert(false, "Should have thrown error for invalid column");
    } catch (error) {
        assert((error as Error).message.includes("Column 'invalid_column' not found"));
    }

    try {
        df.query("age >");
        assert(false, "Should have thrown error for invalid syntax");
    } catch (error) {
        assert((error as Error).message.includes("Invalid condition"));
    }
});

Deno.test("DataFrame - query method: edge cases", () => {
    const data = {
        value: [0, 1, -1, null, undefined],
        text: ["", "hello", "world", null, undefined],
    };
    const df = new DataFrame(data);

    const nullResult = df.query("value == null");
    assertEquals(nullResult.shape, [1, 2]);
    assertEquals(nullResult.index, [3]);

    const zeroResult = df.query("value == 0");
    assertEquals(zeroResult.shape, [1, 2]);
    assertEquals(zeroResult.index, [0]);

    const emptyResult = df.query('text == ""');
    assertEquals(emptyResult.shape, [1, 2]);
    assertEquals(emptyResult.index, [0]);
});

Deno.test("DataFrame - query method: performance with large dataset", () => {
    const size = 10000;
    const data = {
        id: Array.from({ length: size }, (_, i) => i),
        value: Array.from({ length: size }, (_, i) => Math.random() * 100),
        category: Array.from({ length: size }, (_, i) => i % 2 === 0 ? "A" : "B"),
    };
    const df = new DataFrame(data);

    const start = performance.now();
    const result = df.query("value > 50 and category == 'A'");
    const end = performance.now();

    assert(result.shape[0] > 0);
    console.log(`DataFrame query performance: ${(end - start).toFixed(2)}ms for ${size} rows`);
});

Deno.test("DataFrame - Performance: sorting with large dataset", () => {
    const size = 10000;
    const data = generateNumericData(size);
    const df = new DataFrame(data);

    const start = performance.now();
    const sorted = df.sortValues("value");
    const end = performance.now();

    assertEquals(sorted.shape, [size, 4]);
    console.log(`DataFrame sorting performance: ${(end - start).toFixed(2)}ms for ${size} rows`);
});

Deno.test("DataFrame - Performance: two-column sorting with large dataset", () => {
    const size = 5000;
    const data = generateNumericData(size);
    const df = new DataFrame(data);

    const start = performance.now();
    const sorted = df.sortValues(["value", "score"]);
    const end = performance.now();

    assertEquals(sorted.shape, [size, 4]);
    console.log(`DataFrame two-column sorting performance: ${(end - start).toFixed(2)}ms for ${size} rows`);
});

Deno.test("DataFrame - Performance: filtering with large dataset", () => {
    const size = 10000;
    const data = generateNumericData(size);
    const df = new DataFrame(data);

    const mask = Array.from({ length: size }, (_, i) => i % 2 === 0);

    const start = performance.now();
    const filtered = df.filter(mask);
    const end = performance.now();

    assertEquals(filtered.shape, [size / 2, 4]);
    console.log(`DataFrame filtering performance: ${(end - start).toFixed(2)}ms for ${size} rows`);
});

// Test WASM fallback behavior when WASM is disabled
Deno.test("DataFrame - WASM fallback behavior when disabled", () => {
    // Temporarily disable WASM
    const originalValue = globalThis.DF_USE_WASM_ENGINE;
    globalThis.DF_USE_WASM_ENGINE = false;

    try {
        // Test that DataFrame operations still work with JavaScript fallbacks
        const data = {
            name: ["Alice", "Bob", "Charlie", "David"],
            age: [25, 30, 35, 40],
            score: [85, 90, 78, 92],
        };
        const df = new DataFrame(data);

        // Test sorting with JavaScript fallback
        const sorted = df.sortValues("age");
        const sortedAge = sorted.getColumns(["age"]);
        assertEquals(sortedAge.data.get("age")?.values, [25, 30, 35, 40]);

        // Test two-column sorting with JavaScript fallback
        const multiSorted = df.sortValues(["age", "score"]);
        const multiSortedAge = multiSorted.getColumns(["age"]);
        assertEquals(multiSortedAge.data.get("age")?.values, [25, 30, 35, 40]);

        // Test filtering with JavaScript fallback
        const mask = [true, false, true, false];
        const filtered = df.filter(mask);
        assertEquals(filtered.shape, [2, 3]);
        const filteredName = filtered.getColumns(["name"]);
        assertEquals(filteredName.data.get("name")?.values, ["Alice", "Charlie"]);
    } finally {
        // Restore original value
        globalThis.DF_USE_WASM_ENGINE = originalValue;
    }
});

// Test WASM fallback with edge cases
Deno.test("DataFrame - WASM fallback edge cases", () => {
    // Temporarily disable WASM
    const originalValue = globalThis.DF_USE_WASM_ENGINE;
    globalThis.DF_USE_WASM_ENGINE = false;

    try {
        // Test with null values
        const dataWithNulls = {
            name: ["Alice", "Bob", null, "David"],
            age: [25, null, 35, 40],
            score: [85, 90, null, 92],
        };
        const df = new DataFrame(dataWithNulls);

        // Test sorting with null values
        const sorted = df.sortValues("age");
        const sortedAge = sorted.getColumns(["age"]);
        assertEquals(sortedAge.data.get("age")?.values, [25, 35, 40, null]);

        // Test filtering with null values
        const mask = [true, false, true, true];
        const filtered = df.filter(mask);
        assertEquals(filtered.shape, [3, 3]);
        const filteredName = filtered.getColumns(["name"]);
        assertEquals(filteredName.data.get("name")?.values, ["Alice", null, "David"]);

        // Test empty DataFrame
        const emptyDf = new DataFrame({});
        assertEquals(emptyDf.shape, [0, 0]);
    } finally {
        // Restore original value
        globalThis.DF_USE_WASM_ENGINE = originalValue;
    }
});
