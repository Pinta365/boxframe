/**
 * Tests for BoxFrame static methods
 */

import { assertEquals, assertThrows } from "@std/assert";
import { BoxFrame } from "./boxframe.ts";

// Test data generators
function generateTestData(size: number): Record<string, any[]> {
    return {
        id: Array.from({ length: size }, (_, i) => i),
        value: Array.from({ length: size }, () => Math.random() * 1000),
        category: Array.from({ length: size }, () => ["A", "B", "C"][Math.floor(Math.random() * 3)]),
    };
}

Deno.test("BoxFrame - DataFrame creation from arrays", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
        col3: [1.1, 2.2, 3.3],
    };

    const df = BoxFrame.fromRecord(data);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["col1", "col2", "col3"]);
});

Deno.test("BoxFrame - DataFrame creation with options", () => {
    const data = {
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
    };

    const customColumns = ["x", "y"];
    const customIndex = ["row1", "row2", "row3"];

    const df = BoxFrame.fromRecord(data, { columns: customColumns, index: customIndex });

    assertEquals(df.columns, customColumns);
    assertEquals(df.index, customIndex);
});

Deno.test("BoxFrame - readCsv basic functionality", () => {
    const csvData = `name,age,city
John,30,New York
Jane,25,Los Angeles
Bob,35,Chicago`;

    const df = BoxFrame.parseCsv(csvData);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);
});

Deno.test("BoxFrame - readCsv with options", () => {
    const csvData = `John,30,New York
Jane,25,Los Angeles
Bob,35,Chicago`;

    const df = BoxFrame.parseCsv(csvData, {
        columns: ["name", "age", "city"],
        hasHeader: false,
    });

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);
});

Deno.test("BoxFrame - concat with axis=0 (vertical)", () => {
    const df1 = BoxFrame.DataFrame({
        col1: [1, 2],
        col2: ["a", "b"],
    });

    const df2 = BoxFrame.DataFrame({
        col1: [3, 4],
        col2: ["c", "d"],
    });

    const result = BoxFrame.concat([df1, df2], 0);

    assertEquals(result.shape, [4, 2]);
    assertEquals(result.columns, ["col1", "col2"]);
    assertEquals(result.index.length, 4);
});

Deno.test("BoxFrame - concat with axis=1 (horizontal)", () => {
    const df1 = BoxFrame.DataFrame({
        col1: [1, 2],
        col2: ["a", "b"],
    });

    const df2 = BoxFrame.DataFrame({
        col3: [3, 4],
        col4: ["c", "d"],
    });

    const result = BoxFrame.concat([df1, df2], 1);

    assertEquals(result.shape, [2, 4]);
    assertEquals(result.columns, ["col1", "col2", "col3", "col4"]);
});

Deno.test("BoxFrame - concat with different column sets", () => {
    const df1 = BoxFrame.DataFrame({
        col1: [1, 2],
        col2: ["a", "b"],
    });

    const df2 = BoxFrame.DataFrame({
        col1: [3, 4],
        col3: ["c", "d"], // Different column
    });

    const result = BoxFrame.concat([df1, df2], 0);

    assertEquals(result.shape, [4, 3]);
    assertEquals(result.columns, ["col1", "col2", "col3"]);
});

Deno.test("BoxFrame - concat with ignore_index option", () => {
    const df1 = BoxFrame.DataFrame({
        col1: [1, 2],
        col2: ["a", "b"],
    }, { index: ["row1", "row2"] });

    const df2 = BoxFrame.DataFrame({
        col1: [3, 4],
        col2: ["c", "d"],
    }, { index: ["row3", "row4"] });

    const result = BoxFrame.concat([df1, df2], 0, { ignore_index: true });

    assertEquals(result.shape, [4, 2]);
    assertEquals(result.index, [0, 1, 2, 3]);
});

Deno.test("BoxFrame - toNumeric conversion", () => {
    const data = ["1", "2.5", "3.7", "invalid", "5"];
    const result = BoxFrame.toNumeric(data);

    assertEquals(result.dtype, "float64");
    const values = result.values as (number | null)[];
    assertEquals(values[0], 1);
    assertEquals(values[1], 2.5);
    assertEquals(values[2], 3.7);
    assertEquals(values[3], null);
    assertEquals(values[4], 5);
});

Deno.test("BoxFrame - toNumeric with Series input", () => {
    const series = BoxFrame.fromArray(["1", "2.5", "3.7"]);
    const result = BoxFrame.toNumeric(series);

    assertEquals(result.dtype, "float64");
    const values = result.values as number[];
    assertEquals(values, [1, 2.5, 3.7]);
});

Deno.test("BoxFrame - toDatetime conversion", () => {
    const data = ["2023-01-01", "2023-02-15", "2023-03-30"];
    const result = BoxFrame.toDatetime(data);

    assertEquals(result.dtype, "datetime");
    assertEquals(result.length, 3);

    const values = result.values as Date[];
    assertEquals(values[0] instanceof Date, true);
    assertEquals(values[1] instanceof Date, true);
    assertEquals(values[2] instanceof Date, true);
});

Deno.test("BoxFrame - toDatetime with invalid dates", () => {
    const data = ["2023-01-01", "invalid-date", "2023-03-30"];
    const result = BoxFrame.toDatetime(data);

    assertEquals(result.dtype, "datetime");
    assertEquals(result.length, 3);

    const values = result.values as (Date | null)[];
    assertEquals(values[0] instanceof Date, true);
    assertEquals(values[1], null);
    assertEquals(values[2] instanceof Date, true);
});

Deno.test("BoxFrame - cut with numeric data", () => {
    const data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const bins = [0, 3, 6, 10];
    const labels = ["Low", "Medium", "High"];

    const result = BoxFrame.cut(BoxFrame.fromArray(data), bins, labels);

    assertEquals(result.dtype, "string");
    assertEquals(result.length, 10);

    const values = result.values as string[];
    assertEquals(values[0], "Low");
    assertEquals(values[1], "Low");
    assertEquals(values[2], "Low");
    assertEquals(values[3], "Medium");
    assertEquals(values[4], "Medium");
    assertEquals(values[5], "Medium");
    assertEquals(values[6], "High");
    assertEquals(values[7], "High");
    assertEquals(values[8], "High");
    assertEquals(values[9], "High");
});

Deno.test("BoxFrame - cut with Series input", () => {
    const series = BoxFrame.fromArray([1, 2, 3, 4, 5]);
    const bins = [0, 2, 5];
    const labels = ["Low", "High"];

    const result = BoxFrame.cut(series, bins, labels);

    assertEquals(result.dtype, "string");
    assertEquals(result.length, 5);

    const values = result.values as string[];
    assertEquals(values, ["Low", "Low", "High", "High", "High"]);
});

Deno.test("BoxFrame - cut with right=false", () => {
    const data = [1, 2, 3, 4, 5];
    const bins = [1, 3, 5];
    const labels = ["Low", "High"];

    const result = BoxFrame.cut(BoxFrame.fromArray(data), bins, labels);

    const values = result.values as string[];
    assertEquals(values[0], "Low");
    assertEquals(values[1], "Low");
    assertEquals(values[2], "Low");
    assertEquals(values[3], "High");
    assertEquals(values[4], "High");
});

Deno.test("BoxFrame - dateRange with daily frequency", () => {
    const start = new Date("2023-01-01");
    const end = new Date("2023-01-05");

    const result = BoxFrame.dateRange({ start, end, freq: "D" });

    assertEquals(result.length, 5);
    assertEquals(result.dtype, "datetime");

    const values = result.values as Date[];
    assertEquals(values[0].toISOString().split("T")[0], "2023-01-01");
    assertEquals(values[1].toISOString().split("T")[0], "2023-01-02");
    assertEquals(values[2].toISOString().split("T")[0], "2023-01-03");
    assertEquals(values[3].toISOString().split("T")[0], "2023-01-04");
    assertEquals(values[4].toISOString().split("T")[0], "2023-01-05");
});

Deno.test("BoxFrame - dateRange with weekly frequency", () => {
    const start = new Date("2023-01-01");
    const end = new Date("2023-01-22");

    const result = BoxFrame.dateRange({ start, end, freq: "W" });

    assertEquals(result.length, 4);
    assertEquals(result.dtype, "datetime");
});

Deno.test("BoxFrame - dateRange with monthly frequency", () => {
    const start = new Date("2023-01-01");
    const end = new Date("2023-04-01");

    const result = BoxFrame.dateRange({ start, end, freq: "M" });

    assertEquals(result.length, 4);
    assertEquals(result.dtype, "datetime");
});

Deno.test("BoxFrame - dateRange with periods", () => {
    const start = new Date("2023-01-01");

    const result = BoxFrame.dateRange({ start, periods: 5, freq: "D" });

    assertEquals(result.length, 5);
    assertEquals(result.dtype, "datetime");

    const values = result.values as Date[];
    assertEquals(values[0].toISOString().split("T")[0], "2023-01-01");
    assertEquals(values[4].toISOString().split("T")[0], "2023-01-05");
});

Deno.test("BoxFrame - concat with empty array", () => {
    assertThrows(
        () => BoxFrame.concat([]),
        Error,
        "At least one DataFrame must be provided",
    );
});

Deno.test("BoxFrame - concat with invalid axis", () => {
    const df = BoxFrame.DataFrame({ col1: [1, 2] });

    assertThrows(
        () => BoxFrame.concat([df], 2 as any),
        Error,
        "Axis must be 0 or 1",
    );
});

Deno.test("BoxFrame - cut with invalid bins", () => {
    const data = [1, 2, 3, 4, 5];

    assertThrows(
        () => BoxFrame.cut(BoxFrame.fromArray(data), [5, 1]),
        Error,
        "Bins must be in ascending order",
    );
});

Deno.test("BoxFrame - cut with mismatched labels", () => {
    const data = [1, 2, 3, 4, 5];
    const bins = [0, 2, 4, 6];
    const labels = ["Low", "High"];

    assertThrows(
        () => BoxFrame.cut(BoxFrame.fromArray(data), bins, labels),
        Error,
        "Number of labels must match number of bins",
    );
});

Deno.test("BoxFrame - dateRange with invalid frequency", () => {
    const start = new Date("2023-01-01");
    const end = new Date("2023-01-05");

    assertThrows(
        () => BoxFrame.dateRange({ start, end, freq: "X" as any }),
        Error,
        "Unsupported frequency",
    );
});

Deno.test("BoxFrame - dateRange with neither end nor periods", () => {
    const start = new Date("2023-01-01");

    assertThrows(
        () => BoxFrame.dateRange({ start }),
        Error,
        "Either end date or periods must be specified",
    );
});

Deno.test("BoxFrame - Performance: concat with large datasets", () => {
    const size = 5000;
    const df1 = BoxFrame.DataFrame(generateTestData(size));
    const df2 = BoxFrame.DataFrame(generateTestData(size));

    const start = performance.now();
    const result = BoxFrame.concat([df1, df2], 0);
    const end = performance.now();

    assertEquals(result.shape, [size * 2, 3]);
    console.log(`Concat performance: ${(end - start).toFixed(2)}ms for ${size * 2} rows`);
});

Deno.test("BoxFrame - Performance: toNumeric with large dataset", () => {
    const size = 10000;
    const data = Array.from({ length: size }, (_, i) => i.toString());

    const start = performance.now();
    const result = BoxFrame.toNumeric(data);
    const end = performance.now();

    assertEquals(result.length, size);
    assertEquals(result.dtype, "float64");
    console.log(`toNumeric performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});

Deno.test("BoxFrame - Performance: cut with large dataset", () => {
    const size = 10000;
    const data = Array.from({ length: size }, () => Math.random() * 100);
    const bins = [0, 25, 50, 75, 100];
    const labels = ["Q1", "Q2", "Q3", "Q4"];

    const start = performance.now();
    const result = BoxFrame.cut(BoxFrame.fromArray(data), bins, labels);
    const end = performance.now();

    assertEquals(result.length, size);
    assertEquals(result.dtype, "string");
    console.log(`Cut performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});
