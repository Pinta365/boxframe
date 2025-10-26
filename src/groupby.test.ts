/**
 * Tests for GroupBy functionality
 *
 * Tests Series and DataFrame GroupBy operations including aggregation functions
 */

import { assertEquals, assertThrows } from "@std/assert";
import { DataFrame, Series } from "./boxframe.ts";

function generateTestSeries(): Series<number> {
    return new Series([1, 2, 3, 4, 5, 6], {
        name: "values",
        index: ["a", "b", "c", "d", "e", "f"],
        dtype: "float64",
    });
}

function generateTestDataFrame(): DataFrame {
    return new DataFrame({
        category: ["A", "B", "A", "B", "A", "B"],
        value: [10, 20, 30, 40, 50, 60],
        score: [1, 2, 3, 4, 5, 6],
    });
}

Deno.test("Series GroupBy - Basic grouping by array", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    assertEquals(groupBy.groups().size, 2);
    assertEquals(groupBy.groups().get("x"), [0, 2, 4]);
    assertEquals(groupBy.groups().get("y"), [1, 3, 5]);
});

Deno.test("Series GroupBy - Group by function", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy((value) => (value as number) > 3 ? "high" : "low");

    assertEquals(groupBy.groups().size, 2);
    assertEquals(groupBy.groups().get("low"), [0, 1, 2]);
    assertEquals(groupBy.groups().get("high"), [3, 4, 5]);
});

Deno.test("Series GroupBy - Group by index (default)", () => {
    const series = new Series([1, 2, 1, 2], {
        name: "values",
        index: ["a", "b", "a", "c"],
        dtype: "float64",
    });
    const groupBy = series.groupBy();

    assertEquals(groupBy.groups().size, 3);
    assertEquals(groupBy.groups().get("a"), [0, 2]);
    assertEquals(groupBy.groups().get("b"), [1]);
    assertEquals(groupBy.groups().get("c"), [3]);
});

Deno.test("Series GroupBy - Sum aggregation", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);
    const result = groupBy.sum();

    assertEquals(result.values, [9, 12]); // 1+3+5=9, 2+4+6=12
    assertEquals(result.index, ["x", "y"]);
    assertEquals(result.name, "values_sum");
});

Deno.test("Series GroupBy - Mean aggregation", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);
    const result = groupBy.mean();

    assertEquals(result.values, [3, 4]); // (1+3+5)/3=3, (2+4+6)/3=4
    assertEquals(result.index, ["x", "y"]);
    assertEquals(result.name, "values_mean");
});

Deno.test("Series GroupBy - Count aggregation", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);
    const result = groupBy.count();

    assertEquals(result.values, [3, 3]);
    assertEquals(result.index, ["x", "y"]);
    assertEquals(result.name, "values_count");
});

Deno.test("Series GroupBy - Min/Max aggregation", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    const minResult = groupBy.min();
    assertEquals(minResult.values, [1, 2]); // min of [1,3,5] and [2,4,6]

    const maxResult = groupBy.max();
    assertEquals(maxResult.values, [5, 6]); // max of [1,3,5] and [2,4,6]
});

Deno.test("Series GroupBy - Std/Var aggregation", () => {
    const series = new Series([1, 2, 3, 4, 5, 6], {
        name: "values",
        dtype: "float64",
    });
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    const stdResult = groupBy.std();
    assertEquals(stdResult.values.length, 2);
    assertEquals(stdResult.name, "values_std");

    const varResult = groupBy.var();
    assertEquals(varResult.values.length, 2);
    assertEquals(varResult.name, "values_var");
});

Deno.test("Series GroupBy - First/Last aggregation", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    const firstResult = groupBy.first();
    assertEquals(firstResult.values, [1, 2]); // first of [1,3,5] and [2,4,6]

    const lastResult = groupBy.last();
    assertEquals(lastResult.values, [5, 6]); // last of [1,3,5] and [2,4,6]
});

Deno.test("Series GroupBy - Size method", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);
    const result = groupBy.size();

    assertEquals(result.values, [3, 3]);
    assertEquals(result.index, ["x", "y"]);
    assertEquals(result.name, "values_size");
});

Deno.test("Series GroupBy - Get group", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    const xGroup = groupBy.getGroup("x");
    assertEquals(xGroup.values, [1, 3, 5]);
    assertEquals(xGroup.index, ["a", "c", "e"]);

    const yGroup = groupBy.getGroup("y");
    assertEquals(yGroup.values, [2, 4, 6]);
    assertEquals(yGroup.index, ["b", "d", "f"]);
});

Deno.test("Series GroupBy - Get non-existent group", () => {
    const series = generateTestSeries();
    const groupBy = series.groupBy(["x", "y", "x", "y", "x", "y"]);

    assertThrows(() => groupBy.getGroup("z"), Error, "Group 'z' not found");
});

Deno.test("Series GroupBy - Dropna option", () => {
    const series = new Series([1, 2, null, 4, 5, null], {
        name: "values",
        dtype: "float64",
    });

    const groupByWithDropna = series.groupBy(["x", "y", null, "x", "y", null]);
    assertEquals(groupByWithDropna.groups().size, 2);
    assertEquals(groupByWithDropna.groups().get("x"), [0, 3]);
    assertEquals(groupByWithDropna.groups().get("y"), [1, 4]);

    const groupByWithoutDropna = series.groupBy(["x", "y", null, "x", "y", null], { dropna: false });
    assertEquals(groupByWithoutDropna.groups().size, 3);
    assertEquals(groupByWithoutDropna.groups().get("x"), [0, 3]);
    assertEquals(groupByWithoutDropna.groups().get("y"), [1, 4]);
    assertEquals(groupByWithoutDropna.groups().get("null"), [2, 5]);
});

Deno.test("Series GroupBy - Sort option", () => {
    const series = new Series([1, 2, 3, 4], {
        name: "values",
        dtype: "float64",
    });

    const groupBySorted = series.groupBy(["z", "a", "z", "a"]);
    assertEquals(Array.from(groupBySorted.groups().keys()).sort(), ["a", "z"]);

    const groupByUnsorted = series.groupBy(["z", "a", "z", "a"], { sort: false });
    assertEquals(Array.from(groupByUnsorted.groups().keys()), ["z", "a"]);
});

Deno.test("DataFrame GroupBy - Basic grouping by single column", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");

    assertEquals(groupBy.groups().size, 2);
    assertEquals(groupBy.groups().get("A"), [0, 2, 4]);
    assertEquals(groupBy.groups().get("B"), [1, 3, 5]);
});

Deno.test("DataFrame GroupBy - Grouping by multiple columns", () => {
    const df = new DataFrame({
        category: ["A", "A", "B", "B"],
        subcategory: ["X", "Y", "X", "Y"],
        value: [10, 20, 30, 40],
    });
    const groupBy = df.groupBy(["category", "subcategory"]);

    assertEquals(groupBy.groups().size, 4);
    assertEquals(groupBy.groups().get("A|X"), [0]);
    assertEquals(groupBy.groups().get("A|Y"), [1]);
    assertEquals(groupBy.groups().get("B|X"), [2]);
    assertEquals(groupBy.groups().get("B|Y"), [3]);
});

Deno.test("DataFrame GroupBy - Sum aggregation", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");
    const result = groupBy.sum();

    assertEquals(result.shape, [2, 3]);
    assertEquals(result.index, ["A", "B"]);
    assertEquals(result.columns, ["category", "value", "score"]);

    const valueSeries = result.get("value");
    assertEquals(valueSeries.values, [90, 120]); // A: 10+30+50=90, B: 20+40+60=120
});

Deno.test("DataFrame GroupBy - Mean aggregation", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");
    const result = groupBy.mean();

    assertEquals(result.shape, [2, 3]);
    assertEquals(result.index, ["A", "B"]);

    const valueSeries = result.get("value");
    assertEquals(valueSeries.values, [30, 40]); // A: (10+30+50)/3=30, B: (20+40+60)/3=40
});

Deno.test("DataFrame GroupBy - Multiple aggregations", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");
    const result = groupBy.agg({ value: "sum", score: "mean" });

    assertEquals(result.shape, [2, 2]);
    assertEquals(result.index, ["A", "B"]);
    assertEquals(result.columns, ["value_sum", "score_mean"]);

    const valueSumSeries = result.get("value_sum");
    assertEquals(valueSumSeries.values, [90, 120]);

    const scoreMeanSeries = result.get("score_mean");
    assertEquals(scoreMeanSeries.values, [3, 4]); // A: (1+3+5)/3=3, B: (2+4+6)/3=4
});

Deno.test("DataFrame GroupBy - Get group", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");

    const aGroup = groupBy.getGroup("A");
    assertEquals(aGroup.shape, [3, 3]);
    assertEquals(aGroup.index, [0, 2, 4]);

    const bGroup = groupBy.getGroup("B");
    assertEquals(bGroup.shape, [3, 3]);
    assertEquals(bGroup.index, [1, 3, 5]);
});

Deno.test("DataFrame GroupBy - Size method", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");
    const result = groupBy.size();

    assertEquals(result.values, [3, 3]);
    assertEquals(result.index, ["A", "B"]);
    assertEquals(result.name, "size");
});

Deno.test("DataFrame GroupBy - Invalid column", () => {
    const df = generateTestDataFrame();

    assertThrows(() => df.groupBy("invalid_column"), Error, "Column 'invalid_column' not found");
});

Deno.test("DataFrame GroupBy - Invalid aggregation column", () => {
    const df = generateTestDataFrame();
    const groupBy = df.groupBy("category");

    assertThrows(() => groupBy.agg({ invalid_column: "sum" }), Error, "Column 'invalid_column' not found");
});

Deno.test("GroupBy - Empty Series", () => {
    const series = new Series([], { name: "empty", dtype: "float64" });
    const groupBy = series.groupBy([]);

    assertEquals(groupBy.groups().size, 0);
    assertEquals(groupBy.sum().values, []);
});

Deno.test("GroupBy - Single group", () => {
    const series = new Series([1, 2, 3], { name: "values", dtype: "float64" });
    const groupBy = series.groupBy(["x", "x", "x"]);

    assertEquals(groupBy.groups().size, 1);
    assertEquals(groupBy.groups().get("x"), [0, 1, 2]);
    assertEquals(groupBy.sum().values, [6]); // 1+2+3=6
});

Deno.test("GroupBy - All null values", () => {
    const series = new Series([null, null, null], { name: "values", dtype: "float64" });
    const groupBy = series.groupBy(["x", "y", "x"]);

    assertEquals(groupBy.groups().size, 2);
    assertEquals(groupBy.sum().values, [0, 0]);
    assertEquals(groupBy.count().values, [0, 0]);
});

Deno.test("GroupBy - Performance with large dataset", () => {
    const size = 1000;
    const data = Array.from({ length: size }, (_, i) => Math.random() * 100);
    const groups = Array.from({ length: size }, (_, i) => `group_${i % 10}`);

    const series = new Series(data, { name: "values", dtype: "float64" });

    const start = performance.now();
    const groupBy = series.groupBy(groups);
    const result = groupBy.sum();
    const end = performance.now();

    assertEquals(result.values.length, 10);
    console.log(`GroupBy performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});
