/**
 * Tests and demo for CSV streaming with workers
 *
 * Demonstrates worker-based parallel CSV parsing vs single-threaded parsing
 */

import { assertEquals } from "@std/assert";
import { analyzeCsv, parseCsvBatchedStream, parseCsvStream } from "./csv_stream.ts";
import type { DataFrame } from "./dataframe.ts";

// Sample CSV files
const SMALL_CSV = "./samples/Hospital_Patient_Records/patients.csv";

Deno.test({
    name: "CSV Worker Streaming - Basic batched streaming with workers",
    fn: async () => {
        const batches: DataFrame[] = [];
        let rowsProcessed = 0;

        await parseCsvBatchedStream(SMALL_CSV, {
            useWorkers: true,
            workerCount: 2,
            preserveOrder: false,
            batchSize: 100,
            onBatch: (df) => {
                batches.push(df);
                rowsProcessed += df.shape[0];
            },
        });

        // Verify all batches were processed
        assertEquals(batches.length > 0, true, "Should have processed at least one batch");
        assertEquals(rowsProcessed > 0, true, "Should have processed some rows");

        // Verify first batch structure
        const firstBatch = batches[0];
        assertEquals(firstBatch.columns.length > 0, true, "Should have columns");
        assertEquals(firstBatch.shape[0] > 0, true, "Should have rows");
    },
});

Deno.test({
    name: "CSV Worker Streaming - Batched streaming without workers (baseline)",
    fn: async () => {
        const batches: DataFrame[] = [];
        let rowsProcessed = 0;

        await parseCsvBatchedStream(SMALL_CSV, {
            useWorkers: false, // Explicitly disable workers
            batchSize: 100,
            onBatch: (df) => {
                batches.push(df);
                rowsProcessed += df.shape[0];
            },
        });

        assertEquals(batches.length > 0, true, "Should have processed at least one batch");
        assertEquals(rowsProcessed > 0, true, "Should have processed some rows");
    },
});

Deno.test({
    name: "CSV Worker Streaming - Parse stream with workers (preserve order)",
    fn: async () => {
        const result = await parseCsvStream(SMALL_CSV, {
            useWorkers: true,
            workerCount: 2,
            preserveOrder: true, // Default for parseCsvStream
        });

        assertEquals(result.shape[0] > 0, true, "Should have rows");
        assertEquals(result.columns.length > 0, true, "Should have columns");
    },
});

Deno.test({
    name: "CSV Worker Streaming - Schema pre-computation",
    fn: async () => {
        // First, analyze to get schema
        const analysis = await analyzeCsv(SMALL_CSV, { sampleLines: 100 });
        const schema = {
            columns: analysis.columns,
            dtypes: analysis.dtypes as Record<string, "int32" | "float64" | "string" | "bool" | "datetime">,
        };

        // Now parse with pre-computed schema
        const batches: DataFrame[] = [];
        await parseCsvBatchedStream(SMALL_CSV, {
            useWorkers: true,
            schema,
            batchSize: 100,
            onBatch: (df) => {
                batches.push(df);
            },
        });

        assertEquals(batches.length > 0, true, "Should have processed batches");
        const totalRows = batches.reduce((sum, df) => sum + df.shape[0], 0);
        assertEquals(totalRows > 0, true, "Should have processed rows");
    },
});

Deno.test({
    name: "CSV Worker Streaming - Performance comparison demo",
    fn: async () => {
        // Single-threaded
        const startSingle = performance.now();
        let singleRows = 0;
        await parseCsvBatchedStream(SMALL_CSV, {
            useWorkers: false,
            batchSize: 100,
            onBatch: (df) => {
                singleRows += df.shape[0];
            },
        });
        const timeSingle = performance.now() - startSingle;

        // Multi-threaded with workers
        const startWorkers = performance.now();
        let workerRows = 0;
        await parseCsvBatchedStream(SMALL_CSV, {
            useWorkers: true,
            workerCount: 2,
            batchSize: 100,
            onBatch: (df) => {
                workerRows += df.shape[0];
            },
        });
        const timeWorkers = performance.now() - startWorkers;

        console.log(`Single-threaded: ${timeSingle.toFixed(2)}ms (${singleRows} rows)`);
        console.log(`Multi-threaded:  ${timeWorkers.toFixed(2)}ms (${workerRows} rows)`);
        if (timeWorkers > 0) {
            console.log(`Speedup: ${(timeSingle / timeWorkers).toFixed(2)}x`);
        }

        assertEquals(singleRows, workerRows, "Both should process the same number of rows");
    },
});
