/**
 * Tests for CSV parsing functionality
 *
 * Tests the Deno Standard Library CSV implementation
 */

import { assertEquals, assertThrows } from "@std/assert";
import { BoxFrame } from "./boxframe.ts";

const simpleCsv = `name,age,city
John,30,New York
Jane,25,Los Angeles
Bob,35,Chicago`;

const csvWithQuotes = `name,age,description
John,30,"Software Engineer"
Jane,25,"Data Scientist"
Bob,35,"Product Manager"`;

const csvWithCommas = `name,age,description
John,30,"Software Engineer, Senior"
Jane,25,"Data Scientist, ML"
Bob,35,"Product Manager, Lead"`;

const csvWithNewlines = `name,age,description
John,30,"Software Engineer
Senior Level"
Jane,25,"Data Scientist
ML Specialist"
Bob,35,"Product Manager
Team Lead"`;

const csvWithEmptyFields = `name,age,city,email
John,30,New York,john@example.com
Jane,,Los Angeles,
Bob,35,,bob@example.com
,40,Chicago,alice@example.com`;

const csvWithSpecialChars = `name,age,description
José,30,"Software Engineer (Senior)"
François,25,"Data Scientist - ML"
李小明,35,"Product Manager & Lead"`;

const malformedCsv = `name,age,city
John,30,New York
Jane,25,Los Angeles,Extra Field
Bob,35`;

Deno.test("CSV Parser - Basic CSV parsing", () => {
    const df = BoxFrame.parseCsv(simpleCsv);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);

    const nameValues = df.getColumns(["name"]).data.get("name")?.values as string[];
    const ageValues = df.getColumns(["age"]).data.get("age")?.values as string[];
    const cityValues = df.getColumns(["city"]).data.get("city")?.values as string[];

    assertEquals(nameValues, ["John", "Jane", "Bob"]);
    assertEquals(ageValues, ["30", "25", "35"]);
    assertEquals(cityValues, ["New York", "Los Angeles", "Chicago"]);
});

Deno.test("CSV Parser - CSV with quoted fields", () => {
    const df = BoxFrame.parseCsv(csvWithQuotes);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "description"]);

    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];
    assertEquals(descValues, ["Software Engineer", "Data Scientist", "Product Manager"]);
});

Deno.test("CSV Parser - CSV with commas in quoted fields", () => {
    const df = BoxFrame.parseCsv(csvWithCommas);

    assertEquals(df.shape, [3, 3]);

    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];
    assertEquals(descValues, ["Software Engineer, Senior", "Data Scientist, ML", "Product Manager, Lead"]);
});

Deno.test("CSV Parser - CSV with newlines in quoted fields", () => {
    const df = BoxFrame.parseCsv(csvWithNewlines);

    assertEquals(df.shape, [3, 3]);

    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];
    assertEquals(descValues[0], "Software Engineer\nSenior Level");
    assertEquals(descValues[1], "Data Scientist\nML Specialist");
    assertEquals(descValues[2], "Product Manager\nTeam Lead");
});

Deno.test("CSV Parser - CSV with empty fields", () => {
    const df = BoxFrame.parseCsv(csvWithEmptyFields);

    assertEquals(df.shape, [4, 4]);
    assertEquals(df.columns, ["name", "age", "city", "email"]);

    const nameValues = df.getColumns(["name"]).data.get("name")?.values as string[];
    const ageValues = df.getColumns(["age"]).data.get("age")?.values as string[];
    const cityValues = df.getColumns(["city"]).data.get("city")?.values as string[];
    const emailValues = df.getColumns(["email"]).data.get("email")?.values as string[];

    assertEquals(nameValues, ["John", "Jane", "Bob", ""]);
    assertEquals(ageValues, ["30", "", "35", "40"]);
    assertEquals(cityValues, ["New York", "Los Angeles", "", "Chicago"]);
    assertEquals(emailValues, ["john@example.com", "", "bob@example.com", "alice@example.com"]);
});

Deno.test("CSV Parser - CSV with special characters", () => {
    const df = BoxFrame.parseCsv(csvWithSpecialChars);

    assertEquals(df.shape, [3, 3]);

    const nameValues = df.getColumns(["name"]).data.get("name")?.values as string[];
    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];

    assertEquals(nameValues, ["José", "François", "李小明"]);
    assertEquals(descValues, ["Software Engineer (Senior)", "Data Scientist - ML", "Product Manager & Lead"]);
});

Deno.test("CSV Parser - Custom separator", () => {
    const tsvData = `name\tage\tcity
John\t30\tNew York
Jane\t25\tLos Angeles
Bob\t35\tChicago`;

    const df = BoxFrame.parseCsv(tsvData, { delimiter: "\t" });

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);
});

Deno.test("CSV Parser - Custom column names", () => {
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

Deno.test("CSV Parser - Skip first row", () => {
    const csvData = `header1,header2,header3
John,30,New York
Jane,25,Los Angeles
Bob,35,Chicago`;

    const df = BoxFrame.parseCsv(csvData, { hasHeader: true });

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["header1", "header2", "header3"]);
});

Deno.test("CSV Parser - Comments handling", () => {
    const csvWithComments = `# This is a comment
name,age,city
# Another comment
John,30,New York
Jane,25,Los Angeles
# Final comment
Bob,35,Chicago`;

    const df = BoxFrame.parseCsv(csvWithComments, { comment: "#" });

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);
});

Deno.test("CSV Parser - Malformed CSV handling", () => {
    assertThrows(
        () => BoxFrame.parseCsv(malformedCsv),
        Error,
    );
});

Deno.test("CSV Parser - Empty CSV", () => {
    const emptyCsv = "";

    assertThrows(
        () => BoxFrame.parseCsv(emptyCsv),
        Error,
    );
});

Deno.test("CSV Parser - CSV with only headers", () => {
    const headerOnlyCsv = "name,age,city";

    const df = BoxFrame.parseCsv(headerOnlyCsv);

    assertEquals(df.shape, [0, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);
});

Deno.test("CSV Parser - Performance with large dataset", () => {
    const size = 10000;
    const headers = "id,name,age,city,email,score\n";
    const rows = Array.from(
        { length: size },
        (_, i) => `${i},User_${i},${20 + (i % 50)},City_${i % 100},user${i}@example.com,${Math.floor(Math.random() * 100)}`,
    ).join("\n");

    const largeCsv = headers + rows;

    const start = performance.now();
    const df = BoxFrame.parseCsv(largeCsv);
    const end = performance.now();

    assertEquals(df.shape, [size, 6]);
    assertEquals(df.columns, ["id", "name", "age", "city", "email", "score"]);

    const rowsPerSecond = Math.round(size / (end - start) * 1000);
    console.log(`CSV parsing performance: ${(end - start).toFixed(2)}ms for ${size} rows (${rowsPerSecond} rows/sec)`);
});

Deno.test("CSV Parser - CSV with only commas", () => {
    const commaOnlyCsv = ",,,\n,,,\n,,,";

    const df = BoxFrame.parseCsv(commaOnlyCsv);

    assertEquals(df.shape, [2, 1]);
});

Deno.test("CSV Parser - CSV with mixed quote styles", () => {
    const mixedQuotesCsv = `name,age,description
John,30,"Software Engineer"
Jane,25,'Data Scientist'
Bob,35,"Product Manager"`;

    const df = BoxFrame.parseCsv(mixedQuotesCsv);

    assertEquals(df.shape, [3, 3]);
    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];
    assertEquals(descValues, ["Software Engineer", "'Data Scientist'", "Product Manager"]);
});

Deno.test("CSV Parser - CSV with escaped quotes", () => {
    const escapedQuotesCsv = `name,age,description
John,30,"Software ""Senior"" Engineer"
Jane,25,"Data Scientist"
Bob,35,"Product Manager"`;

    const df = BoxFrame.parseCsv(escapedQuotesCsv);

    assertEquals(df.shape, [3, 3]);
    const descValues = df.getColumns(["description"]).data.get("description")?.values as string[];
    assertEquals(descValues[0], 'Software "Senior" Engineer');
});

Deno.test("CSV Parser - Integration with DataFrame operations", () => {
    const df = BoxFrame.parseCsv(simpleCsv);

    const sorted = df.sortValues("age");
    const ageValues = sorted.getColumns(["age"]).data.get("age")?.values as string[];

    // Note: age values are strings, so sorting is lexicographic
    assertEquals(ageValues, ["25", "30", "35"]);
});

Deno.test("CSV Parser - Integration with filtering", () => {
    const df = BoxFrame.parseCsv(simpleCsv);

    // Filter rows where age is greater than 25 (as string comparison)
    const mask = df.getColumns(["age"]).data.get("age")?.values.map((age: string) => age > "25") as boolean[];
    const filtered = df.filter(mask);

    assertEquals(filtered.shape[0], 2);
});
