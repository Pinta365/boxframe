/**
 * Tests for type utilities and data type inference
 *
 * Tests type inference, conversion, and utility functions
 */

import { assertEquals } from "@std/assert";
import { fromTypedArray, inferDType, toTypedArray } from "./types.ts";

Deno.test("Type Utils - inferDType for integers", () => {
    assertEquals(inferDType(42), "int32");
    assertEquals(inferDType(0), "int32");
    assertEquals(inferDType(-100), "int32");
    assertEquals(inferDType(Number.MAX_SAFE_INTEGER), "int32");
    assertEquals(inferDType(Number.MIN_SAFE_INTEGER), "int32");
});

Deno.test("Type Utils - inferDType for floats", () => {
    assertEquals(inferDType(3.14), "float64");
    assertEquals(inferDType(0.0), "int32");
    assertEquals(inferDType(-2.5), "float64");
    assertEquals(inferDType(Number.EPSILON), "float64");
    assertEquals(inferDType(Number.POSITIVE_INFINITY), "float64");
    assertEquals(inferDType(Number.NEGATIVE_INFINITY), "float64");
    assertEquals(inferDType(NaN), "float64");
});

Deno.test("Type Utils - inferDType for strings", () => {
    assertEquals(inferDType("hello"), "string");
    assertEquals(inferDType(""), "string");
    assertEquals(inferDType("123"), "string");
    assertEquals(inferDType("3.14"), "string");
    assertEquals(inferDType("true"), "string");
});

Deno.test("Type Utils - inferDType for booleans", () => {
    assertEquals(inferDType(true), "bool");
    assertEquals(inferDType(false), "bool");
});

Deno.test("Type Utils - inferDType for dates", () => {
    const date = new Date();
    assertEquals(inferDType(date), "datetime");
});

Deno.test("Type Utils - inferDType for null and undefined", () => {
    assertEquals(inferDType(null), "string");
    assertEquals(inferDType(undefined), "string");
});

// Typed array conversion tests
Deno.test("Type Utils - toTypedArray for int32", () => {
    const data = [1, 2, 3, 4, 5];
    const result = toTypedArray(data, "int32");

    assertEquals(result instanceof Int32Array, true);
    assertEquals(result.length, 5);
    assertEquals(Array.from(result), [1, 2, 3, 4, 5]);
});

Deno.test("Type Utils - toTypedArray for float64", () => {
    const data = [1.1, 2.2, 3.3, 4.4, 5.5];
    const result = toTypedArray(data, "float64");

    assertEquals(result instanceof Float64Array, true);
    assertEquals(result.length, 5);
    assertEquals(Array.from(result), [1.1, 2.2, 3.3, 4.4, 5.5]);
});

Deno.test("Type Utils - toTypedArray for string", () => {
    const data = ["a", "b", "c", "d", "e"];
    const result = toTypedArray(data, "string");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, ["a", "b", "c", "d", "e"]);
});

Deno.test("Type Utils - toTypedArray for bool", () => {
    const data = [true, false, true, false, true];
    const result = toTypedArray(data, "bool");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, [true, false, true, false, true]);
});

Deno.test("Type Utils - toTypedArray for datetime", () => {
    const date1 = new Date("2023-01-01");
    const date2 = new Date("2023-01-02");
    const date3 = new Date("2023-01-03");
    const data = [date1, date2, date3];
    const result = toTypedArray(data, "datetime");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 3);
    assertEquals(result, [date1, date2, date3]);
});

Deno.test("Type Utils - toTypedArray with null values", () => {
    const data = [1, null, 3, null, 5];
    const result = toTypedArray(data, "int32");

    // Should return regular array when nulls are present
    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, [1, null, 3, null, 5]);
});

Deno.test("Type Utils - toTypedArray with mixed types", () => {
    const data = [1, "a", 2.5, true];
    const result = toTypedArray(data, "string");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 4);
    assertEquals(result, [1, "a", 2.5, true]);
});

// From typed array conversion tests
Deno.test("Type Utils - fromTypedArray for Int32Array", () => {
    const typedArray = new Int32Array([1, 2, 3, 4, 5]);
    const result = fromTypedArray(typedArray, "int32");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, [1, 2, 3, 4, 5]);
});

Deno.test("Type Utils - fromTypedArray for Float64Array", () => {
    const typedArray = new Float64Array([1.1, 2.2, 3.3, 4.4, 5.5]);
    const result = fromTypedArray(typedArray, "float64");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, [1.1, 2.2, 3.3, 4.4, 5.5]);
});

Deno.test("Type Utils - fromTypedArray for regular array", () => {
    const regularArray = ["a", "b", "c", "d", "e"];
    const result = fromTypedArray(regularArray, "string");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, ["a", "b", "c", "d", "e"]);
});

Deno.test("Type Utils - fromTypedArray for boolean array", () => {
    const boolArray = [true, false, true, false, true];
    const result = fromTypedArray(boolArray, "bool");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 5);
    assertEquals(result, [true, false, true, false, true]);
});

Deno.test("Type Utils - fromTypedArray for datetime array", () => {
    const date1 = new Date("2023-01-01");
    const date2 = new Date("2023-01-02");
    const date3 = new Date("2023-01-03");
    const dateArray = [date1, date2, date3];
    const result = fromTypedArray(dateArray, "datetime");

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, 3);
    assertEquals(result, [date1, date2, date3]);
});

// Edge cases
Deno.test("Type Utils - toTypedArray with empty array", () => {
    const emptyInt = toTypedArray([], "int32");
    const emptyFloat = toTypedArray([], "float64");
    const emptyString = toTypedArray([], "string");

    assertEquals(emptyInt instanceof Int32Array, true);
    assertEquals(emptyInt.length, 0);

    assertEquals(emptyFloat instanceof Float64Array, true);
    assertEquals(emptyFloat.length, 0);

    assertEquals(Array.isArray(emptyString), true);
    assertEquals(emptyString.length, 0);
});

Deno.test("Type Utils - fromTypedArray with empty array", () => {
    const emptyInt = fromTypedArray(new Int32Array([]), "int32");
    const emptyFloat = fromTypedArray(new Float64Array([]), "float64");
    const emptyString = fromTypedArray([], "string");

    assertEquals(Array.isArray(emptyInt), true);
    assertEquals(emptyInt.length, 0);

    assertEquals(Array.isArray(emptyFloat), true);
    assertEquals(emptyFloat.length, 0);

    assertEquals(Array.isArray(emptyString), true);
    assertEquals(emptyString.length, 0);
});

Deno.test("Type Utils - toTypedArray with large numbers", () => {
    const largeNumbers = [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
    const result = toTypedArray(largeNumbers, "float64");

    assertEquals(result instanceof Float64Array, true);
    assertEquals(result.length, 2);
    assertEquals(Array.from(result), [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER]);
});

Deno.test("Type Utils - toTypedArray with very small floats", () => {
    const smallFloats = [Number.EPSILON, Number.MIN_VALUE];
    const result = toTypedArray(smallFloats, "float64");

    assertEquals(result instanceof Float64Array, true);
    assertEquals(result.length, 2);
    assertEquals(Array.from(result), [Number.EPSILON, Number.MIN_VALUE]);
});

Deno.test("Type Utils - toTypedArray with special float values", () => {
    const specialFloats = [Infinity, -Infinity, NaN];
    const result = toTypedArray(specialFloats, "float64");

    assertEquals(result instanceof Float64Array, true);
    assertEquals(result.length, 3);
    assertEquals(result[0], Infinity);
    assertEquals(result[1], -Infinity);
    assertEquals(Number.isNaN(result[2]), true);
});

// Type validation tests
Deno.test("Type Utils - inferDType with edge case numbers", () => {
    // Test boundary cases
    assertEquals(inferDType(Number.MAX_SAFE_INTEGER), "int32");
    assertEquals(inferDType(Number.MIN_SAFE_INTEGER), "int32");
    assertEquals(inferDType(Number.MAX_SAFE_INTEGER + 1), "float64");
    assertEquals(inferDType(Number.MIN_SAFE_INTEGER - 1), "float64");

    // Test zero cases
    assertEquals(inferDType(0), "int32");
    assertEquals(inferDType(-0), "int32");
    assertEquals(inferDType(0.0), "int32"); // 0.0 and 0 are the same value in JavaScript
    assertEquals(inferDType(-0.0), "int32"); // -0.0 and -0 are the same value in JavaScript
});

Deno.test("Type Utils - inferDType with string representations of numbers", () => {
    assertEquals(inferDType("42"), "string");
    assertEquals(inferDType("3.14"), "string");
    assertEquals(inferDType("true"), "string");
    assertEquals(inferDType("false"), "string");
    assertEquals(inferDType("null"), "string");
    assertEquals(inferDType("undefined"), "string");
});

// Performance tests
Deno.test("Type Utils - Performance: toTypedArray with large dataset", () => {
    const size = 100000;
    const data = Array.from({ length: size }, (_, i) => i);

    const start = performance.now();
    const result = toTypedArray(data, "int32");
    const end = performance.now();

    assertEquals(result instanceof Int32Array, true);
    assertEquals(result.length, size);
    console.log(`toTypedArray performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});

Deno.test("Type Utils - Performance: fromTypedArray with large dataset", () => {
    const size = 100000;
    const typedArray = new Int32Array(Array.from({ length: size }, (_, i) => i));

    const start = performance.now();
    const result = fromTypedArray(typedArray, "int32");
    const end = performance.now();

    assertEquals(Array.isArray(result), true);
    assertEquals(result.length, size);
    console.log(`fromTypedArray performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});

Deno.test("Type Utils - Performance: inferDType with large dataset", () => {
    const size = 100000;
    const data = Array.from({ length: size }, (_, i) => i % 2 === 0 ? i : i.toString());

    const start = performance.now();
    for (const value of data) {
        inferDType(value);
    }
    const end = performance.now();

    console.log(`inferDType performance: ${(end - start).toFixed(2)}ms for ${size} elements`);
});
