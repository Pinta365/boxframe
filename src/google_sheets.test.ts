import { assertEquals, assertThrows } from "https://deno.land/std@0.208.0/assert/mod.ts";
import { GoogleSheets } from "./google_sheets.ts";

Deno.test("GoogleSheets - extractId from URL", () => {
    const url = "https://docs.google.com/spreadsheets/d/1ABC123DEF456/edit#gid=0";
    const id = GoogleSheets.extractId(url);
    assertEquals(id, "1ABC123DEF456");
});

Deno.test("GoogleSheets - extractId from URL with query params", () => {
    const url = "https://docs.google.com/spreadsheets/d/1ABC123DEF456/edit?usp=sharing";
    const id = GoogleSheets.extractId(url);
    assertEquals(id, "1ABC123DEF456");
});

Deno.test("GoogleSheets - extractId from invalid URL", () => {
    const url = "https://example.com/not-a-sheets-url";
    assertThrows(() => GoogleSheets.extractId(url), Error, "Invalid Google Sheets URL format");
});

Deno.test("GoogleSheets - extractId from empty string", () => {
    assertThrows(() => GoogleSheets.extractId(""), Error, "Invalid Google Sheets URL format");
});

Deno.test("GoogleSheets - readSheet with invalid ID", async () => {
    try {
        await GoogleSheets.readSheet("");
        assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        assertEquals(errorMessage.includes("Spreadsheet ID must be a non-empty string"), true);
    }
});

Deno.test("GoogleSheets - readSheet with invalid ID type", async () => {
    try {
        await GoogleSheets.readSheet(null as unknown as string);
        assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        assertEquals(errorMessage.includes("Spreadsheet ID must be a non-empty string"), true);
    }
});

// Note: We don't test actual Google Sheets reading here since it requires network access
// and a real spreadsheet. Those tests would be integration tests.
