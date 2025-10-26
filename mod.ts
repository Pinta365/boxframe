/**
 * BoxFrame - A data analysis library for JavaScript with WASM acceleration for performance-critical operations.
 *
 * All operations have JavaScript fallbacks when WASM is disabled.
 */

export * from "./src/types.ts";

export { Series } from "./src/series.ts";
export { DataFrame } from "./src/dataframe.ts";

export { parseCsv, parseCsvFromFile } from "./src/csv_parser.ts";
export type { CsvParseOptions } from "./src/csv_parser.ts";

export { BoxFrame } from "./src/boxframe.ts";
export { WasmEngine } from "./src/wasm_engine.ts";

export { GoogleSheets } from "./src/google_sheets.ts";
