/**
 * Google Sheets integration for BoxFrame
 * Provides methods to read data from Google Sheets
 */

import type { DataFrame } from "./dataframe.ts";
import type { CsvOptions } from "./types.ts";
import { parseCsvFromFile } from "./csv_parser.ts";

/**
 * Google Sheets integration class
 */
export class GoogleSheets {
    /**
     * Read Google Sheets as CSV (public sheets only)
     *
     * @param spreadsheetId - The Google Sheets ID from the URL
     * @param sheetName - Name of the sheet (default: "Sheet1")
     * @param options - CSV parsing options
     * @returns Promise<DataFrame>
     *
     * @example
     * ```typescript
     * // From URL: https://docs.google.com/spreadsheets/d/1ABC123.../edit
     * const df = await GoogleSheets.readSheet("1ABC123...");
     *
     * // Specific sheet
     * const df = await GoogleSheets.readSheet("1ABC123...", "Data");
     *
     * // With CSV options
     * const df = await GoogleSheets.readSheet("1ABC123...", "Sheet1", {
     *   delimiter: ",",
     *   hasHeader: true
     * });
     * ```
     */
    static async readSheet(
        spreadsheetId: string,
        sheetName: string = "Sheet1",
        options?: CsvOptions,
    ): Promise<DataFrame> {
        try {
            // Validate spreadsheet ID
            if (!spreadsheetId || typeof spreadsheetId !== "string") {
                throw new Error("Spreadsheet ID must be a non-empty string");
            }

            // Clean the spreadsheet ID (remove any extra path components)
            const cleanId = spreadsheetId.split("/").pop()?.split("?")[0];
            if (!cleanId) {
                throw new Error("Invalid spreadsheet ID format");
            }

            // Use Google's CSV export URL
            const csvUrl = `https://docs.google.com/spreadsheets/d/${cleanId}/gviz/tq?tqx=out:csv&sheet=${encodeURIComponent(sheetName)}`;

            return await parseCsvFromFile(csvUrl, options);
        } catch (error) {
            if (error instanceof Error && error.message.includes("Invalid spreadsheet ID")) {
                throw error;
            }
            throw new Error(`Failed to read Google Sheet: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    /**
     * Extract spreadsheet ID from Google Sheets URL
     *
     * @param url - Google Sheets URL
     * @returns The spreadsheet ID
     *
     * @example
     * ```typescript
     * const id = GoogleSheets.extractId("https://docs.google.com/spreadsheets/d/1ABC123.../edit");
     * // Returns: "1ABC123..."
     * ```
     */
    static extractId(url: string): string {
        const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
        if (!match || !match[1]) {
            throw new Error("Invalid Google Sheets URL format");
        }
        return match[1];
    }

    /**
     * Read Google Sheets from URL (convenience method)
     *
     * @param url - Full Google Sheets URL
     * @param sheetName - Name of the sheet (default: "Sheet1")
     * @param options - CSV parsing options
     * @returns Promise<DataFrame>
     *
     * @example
     * ```typescript
     * const df = await GoogleSheets.readFromUrl(
     *   "https://docs.google.com/spreadsheets/d/1ABC123.../edit",
     *   "Data"
     * );
     * ```
     */
    static async readFromUrl(
        url: string,
        sheetName: string = "Sheet1",
        options?: CsvOptions,
    ): Promise<DataFrame> {
        const spreadsheetId = this.extractId(url);
        return await this.readSheet(spreadsheetId, sheetName, options);
    }
}
