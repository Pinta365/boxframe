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

Deno.test("CSV Parser - Basic CSV parsing with auto-inference", () => {
    const df = BoxFrame.parseCsv(simpleCsv);

    assertEquals(df.shape, [3, 3]);
    assertEquals(df.columns, ["name", "age", "city"]);

    const nameValues = df.getColumns(["name"]).data.get("name")?.values as string[];
    const ageValues = df.getColumns(["age"]).data.get("age")?.values as number[];
    const cityValues = df.getColumns(["city"]).data.get("city")?.values as string[];

    assertEquals(nameValues, ["John", "Jane", "Bob"]);
    assertEquals(ageValues, [30, 25, 35]);
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

    const nameValues = df.getColumns(["name"]).data.get("name")?.values as (string | null)[];
    const ageValues = df.getColumns(["age"]).data.get("age")?.values as (number | null)[];
    const cityValues = df.getColumns(["city"]).data.get("city")?.values as (string | null)[];
    const emailValues = df.getColumns(["email"]).data.get("email")?.values as (string | null)[];

    assertEquals(nameValues, ["John", "Jane", "Bob", null]);
    assertEquals(ageValues, [30, null, 35, 40]);
    assertEquals(cityValues, ["New York", "Los Angeles", null, "Chicago"]);
    assertEquals(emailValues, ["john@example.com", null, "bob@example.com", "alice@example.com"]);
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
    const ageValues = sorted.getColumns(["age"]).data.get("age")?.values as number[];

    assertEquals(ageValues, [25, 30, 35]);
});

Deno.test("CSV Parser - Integration with filtering", () => {
    const df = BoxFrame.parseCsv(simpleCsv);

    const mask = df.getColumns(["age"]).data.get("age")?.values.map((age: number) => age > 25) as boolean[];
    const filtered = df.filter(mask);

    assertEquals(filtered.shape[0], 2);
});

const csvWithMixedTypes = `name,age,salary,is_active,hire_date
John,25,50000.50,true,2023-01-15
Jane,30,75000,false,2022-06-10
Bob,35,90000.75,true,2021-03-22`;

const csvWithNAValues = `name,age,salary,is_active
John,25,50000.50,true
Jane,30,N/A,false
Bob,35,90000.75,unknown
Alice,28,65000,1`;

Deno.test("CSV Parser - Data type inference", () => {
    const df = BoxFrame.parseCsv(csvWithMixedTypes);

    assertEquals(df.shape, [3, 5]);

    // Check that types are inferred correctly
    const nameSeries = df.data.get("name");
    const ageSeries = df.data.get("age");
    const salarySeries = df.data.get("salary");
    const isActiveSeries = df.data.get("is_active");
    const hireDateSeries = df.data.get("hire_date");

    assertEquals(nameSeries?.dtype, "string");
    assertEquals(ageSeries?.dtype, "int32");
    assertEquals(salarySeries?.dtype, "float64"); // Will be float64 due to decimal
    assertEquals(isActiveSeries?.dtype, "string"); // Will be string due to text
    assertEquals(hireDateSeries?.dtype, "datetime"); // Will be datetime due to date pattern

    // Check values
    assertEquals(nameSeries?.values, ["John", "Jane", "Bob"]);
    assertEquals(ageSeries?.values, [25, 30, 35]);
});

Deno.test("CSV Parser - Explicit data types by column name", () => {
    const df = BoxFrame.parseCsv(csvWithMixedTypes, {
        dtypes: {
            name: "string",
            age: "int32",
            salary: "float64",
            is_active: "bool",
            hire_date: "datetime",
        },
    });

    assertEquals(df.shape, [3, 5]);

    const nameSeries = df.data.get("name");
    const ageSeries = df.data.get("age");
    const salarySeries = df.data.get("salary");
    const isActiveSeries = df.data.get("is_active");
    const hireDateSeries = df.data.get("hire_date");

    assertEquals(nameSeries?.dtype, "string");
    assertEquals(ageSeries?.dtype, "int32");
    assertEquals(salarySeries?.dtype, "float64");
    assertEquals(isActiveSeries?.dtype, "bool");
    assertEquals(hireDateSeries?.dtype, "datetime");

    // Check values
    assertEquals(nameSeries?.values, ["John", "Jane", "Bob"]);
    assertEquals(ageSeries?.values, [25, 30, 35]);
    assertEquals(salarySeries?.values, [50000.5, 75000, 90000.75]);
    assertEquals(isActiveSeries?.values, [true, false, true]);
    assertEquals(hireDateSeries?.values[0] instanceof Date, true);
});

Deno.test("CSV Parser - Explicit data types by column index", () => {
    const csvNoHeader = `John,25,50000.50,true,2023-01-15
Jane,30,75000,false,2022-06-10
Bob,35,90000.75,true,2021-03-22`;

    const df = BoxFrame.parseCsv(csvNoHeader, {
        hasHeader: false,
        dtypes: ["string", "int32", "float64", "bool", "datetime"],
    });

    assertEquals(df.shape, [3, 5]);
    assertEquals(df.columns, ["col_0", "col_1", "col_2", "col_3", "col_4"]);

    const col0Series = df.data.get("col_0");
    const col1Series = df.data.get("col_1");
    const col2Series = df.data.get("col_2");
    const col3Series = df.data.get("col_3");
    const col4Series = df.data.get("col_4");

    assertEquals(col0Series?.dtype, "string");
    assertEquals(col1Series?.dtype, "int32");
    assertEquals(col2Series?.dtype, "float64");
    assertEquals(col3Series?.dtype, "bool");
    assertEquals(col4Series?.dtype, "datetime");

    assertEquals(col0Series?.values, ["John", "Jane", "Bob"]);
    assertEquals(col1Series?.values, [25, 30, 35]);
    assertEquals(col2Series?.values, [50000.5, 75000, 90000.75]);
    assertEquals(col3Series?.values, [true, false, true]);
});

Deno.test("CSV Parser - Disable type inference", () => {
    const df = BoxFrame.parseCsv(csvWithMixedTypes, {
        inferTypes: false,
    });

    assertEquals(df.shape, [3, 5]);

    // All columns should be string type
    const nameSeries = df.data.get("name");
    const ageSeries = df.data.get("age");
    const salarySeries = df.data.get("salary");
    const isActiveSeries = df.data.get("is_active");
    const hireDateSeries = df.data.get("hire_date");

    assertEquals(nameSeries?.dtype, "string");
    assertEquals(ageSeries?.dtype, "string");
    assertEquals(salarySeries?.dtype, "string");
    assertEquals(isActiveSeries?.dtype, "string");
    assertEquals(hireDateSeries?.dtype, "string");

    // All values should be strings
    assertEquals(ageSeries?.values, ["25", "30", "35"]);
    assertEquals(salarySeries?.values, ["50000.50", "75000", "90000.75"]);
    assertEquals(isActiveSeries?.values, ["true", "false", "true"]);
});

Deno.test("CSV Parser - Custom NA values", () => {
    const df = BoxFrame.parseCsv(csvWithNAValues, {
        naValues: ["N/A", "unknown"],
        dtypes: {
            name: "string",
            age: "int32",
            salary: "float64",
            is_active: "bool",
        },
    });

    assertEquals(df.shape, [4, 4]);

    const nameSeries = df.data.get("name");
    const ageSeries = df.data.get("age");
    const salarySeries = df.data.get("salary");
    const isActiveSeries = df.data.get("is_active");

    assertEquals(nameSeries?.values, ["John", "Jane", "Bob", "Alice"]);
    assertEquals(ageSeries?.values, [25, 30, 35, 28]);
    assertEquals(salarySeries?.values, [50000.5, null, 90000.75, 65000]);
    assertEquals(isActiveSeries?.values, [true, false, null, true]); // "unknown" converts to null, "1" converts to true
});

Deno.test("CSV Parser - Boolean type conversion", () => {
    const csvWithBooleans = `name,is_active,is_verified,is_premium
John,true,1,yes
Jane,false,0,no
Bob,TRUE,1,YES
Alice,FALSE,0,NO`;

    const df = BoxFrame.parseCsv(csvWithBooleans, {
        dtypes: {
            name: "string",
            is_active: "bool",
            is_verified: "bool",
            is_premium: "bool",
        },
    });

    assertEquals(df.shape, [4, 4]);

    const isActiveSeries = df.data.get("is_active");
    const isVerifiedSeries = df.data.get("is_verified");
    const isPremiumSeries = df.data.get("is_premium");

    assertEquals(isActiveSeries?.dtype, "bool");
    assertEquals(isVerifiedSeries?.dtype, "bool");
    assertEquals(isPremiumSeries?.dtype, "bool");

    assertEquals(isActiveSeries?.values, [true, false, true, false]);
    assertEquals(isVerifiedSeries?.values, [true, false, true, false]);
    assertEquals(isPremiumSeries?.values, [true, false, true, false]);
});

Deno.test("CSV Parser - DateTime type conversion", () => {
    const csvWithDates = `name,birth_date,hire_date,last_login
John,1990-01-15,2023-01-15,2024-01-15T10:30:00Z
Jane,1985-06-10,2022-06-10,2024-01-14T15:45:00Z
Bob,1980-03-22,2021-03-22,2024-01-13T09:15:00Z`;

    const df = BoxFrame.parseCsv(csvWithDates, {
        dtypes: {
            name: "string",
            birth_date: "datetime",
            hire_date: "datetime",
            last_login: "datetime",
        },
    });

    assertEquals(df.shape, [3, 4]);

    const birthDateSeries = df.data.get("birth_date");
    const hireDateSeries = df.data.get("hire_date");
    const lastLoginSeries = df.data.get("last_login");

    assertEquals(birthDateSeries?.dtype, "datetime");
    assertEquals(hireDateSeries?.dtype, "datetime");
    assertEquals(lastLoginSeries?.dtype, "datetime");

    // Check that all values are Date objects
    assertEquals(birthDateSeries?.values.every((d) => d instanceof Date), true);
    assertEquals(hireDateSeries?.values.every((d) => d instanceof Date), true);
    assertEquals(lastLoginSeries?.values.every((d) => d instanceof Date), true);
});

Deno.test("CSV Parser - Mixed array and object dtypes", () => {
    const csvData = `name,age,salary
John,25,50000.50
Jane,30,75000
Bob,35,90000.75`;

    // Test with array dtypes
    const df1 = BoxFrame.parseCsv(csvData, {
        dtypes: ["string", "int32", "float64"],
    });

    assertEquals(df1.data.get("name")?.dtype, "string");
    assertEquals(df1.data.get("age")?.dtype, "int32");
    assertEquals(df1.data.get("salary")?.dtype, "float64");

    // Test with object dtypes
    const df2 = BoxFrame.parseCsv(csvData, {
        dtypes: {
            name: "string",
            age: "int32",
            salary: "float64",
        },
    });

    assertEquals(df2.data.get("name")?.dtype, "string");
    assertEquals(df2.data.get("age")?.dtype, "int32");
    assertEquals(df2.data.get("salary")?.dtype, "float64");
});

Deno.test("CSV Parser - DateTime inference", () => {
    const csvWithDates = `name,hire_date,last_login,birth_date,description
John,2023-01-15,2024-01-15T10:30:00Z,1990-01-15,Software Engineer
Jane,2022-06-10,2024-01-14T15:45:00Z,1985-06-10,Data Scientist
Bob,2021-03-22,2024-01-13T09:15:00Z,1980-03-22,Product Manager`;

    const df = BoxFrame.parseCsv(csvWithDates);

    assertEquals(df.shape, [3, 5]);

    // Check that date columns are inferred as datetime
    assertEquals(df.data.get("hire_date")?.dtype, "datetime");
    assertEquals(df.data.get("last_login")?.dtype, "datetime");
    assertEquals(df.data.get("birth_date")?.dtype, "datetime");
    assertEquals(df.data.get("description")?.dtype, "string");

    // Check that values are Date objects
    const hireDateValues = df.data.get("hire_date")?.values;
    assertEquals(hireDateValues?.every((d) => d instanceof Date), true);

    // Check that non-date columns remain strings
    const descValues = df.data.get("description")?.values;
    assertEquals(descValues?.every((d) => typeof d === "string"), true);
});

Deno.test("CSV Parser - Mixed valid/invalid dates fallback to string", () => {
    const csvMixedDates = `valid_dates,mixed_dates,invalid_dates
2023-01-15,2023-01-15,not a date
2023-02-20,invalid,also not a date
2023-03-25,2023-03-25,still not a date`;

    const df = BoxFrame.parseCsv(csvMixedDates);

    assertEquals(df.shape, [3, 3]);

    // Only columns with all valid dates should be datetime
    assertEquals(df.data.get("valid_dates")?.dtype, "datetime");
    assertEquals(df.data.get("mixed_dates")?.dtype, "string");
    assertEquals(df.data.get("invalid_dates")?.dtype, "string");
});

Deno.test("CSV Parser - Partial type specification with inference", () => {
    const csvData = `name,age,salary,is_active,hire_date
John,25,50000.50,true,2023-01-15
Jane,30,75000,false,2022-06-10
Bob,35,90000.75,true,2021-03-22`;

    // Specify only some columns, let others be inferred
    const df = BoxFrame.parseCsv(csvData, {
        dtypes: {
            name: "string",
            age: "int32",
            // salary, is_active, hire_date will be inferred
        },
    });

    assertEquals(df.shape, [3, 5]);

    // Check specified types
    assertEquals(df.data.get("name")?.dtype, "string");
    assertEquals(df.data.get("age")?.dtype, "int32");

    // Check inferred types
    assertEquals(df.data.get("salary")?.dtype, "float64"); // inferred as float64 due to decimal
    assertEquals(df.data.get("is_active")?.dtype, "string"); // inferred as string due to text
    assertEquals(df.data.get("hire_date")?.dtype, "datetime"); // inferred as datetime due to date pattern

    // Check values
    assertEquals(df.data.get("name")?.values, ["John", "Jane", "Bob"]);
    assertEquals(df.data.get("age")?.values, [25, 30, 35]);
});

Deno.test("CSV Parser - Error handling for invalid types", () => {
    const csvData = `name,age
John,25
Jane,30`;

    // This should not throw an error, but should fall back to string
    const df = BoxFrame.parseCsv(csvData, {
        dtypes: {
            name: "invalid_type" as any,
            age: "int32",
        },
    });

    assertEquals(df.data.get("name")?.dtype, "string");
    assertEquals(df.data.get("age")?.dtype, "int32");
});
