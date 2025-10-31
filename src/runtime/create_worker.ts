/**
 * Cross-runtime worker creation helper
 * Supports Node.js (worker_threads), Deno, and Browser workers
 */

import { CurrentRuntime, Runtime } from "@cross/runtime";

/**
 * Worker-like interface that abstracts over different runtime implementations
 */
export interface WorkerLike {
    postMessage(message: unknown, transfer?: Transferable[]): void;
    onmessage: ((event: MessageEvent) => void) | null;
    onerror: ((event: ErrorEvent) => void) | null;
    terminate?: () => void | Promise<void>;
    close?: () => void;
}

/**
 * Create a worker instance appropriate for the current runtime
 */
export async function createWorker(moduleUrl: string | URL): Promise<WorkerLike> {
    const url = typeof moduleUrl === "string" ? new URL(moduleUrl, import.meta.url) : moduleUrl;

    if (CurrentRuntime === Runtime.Node) {
        // Node.js: Use worker_threads
        const { Worker } = await import("node:worker_threads");
        const process = await import("node:process");
        let filePath: string;
        if (url.protocol === "file:") {
            filePath = url.pathname;
            // On Windows, remove leading slash from file:///C:/path -> C:/path
            if (process.platform === "win32" && filePath.startsWith("/")) {
                filePath = filePath.slice(1);
            }
        } else {
            filePath = url.href;
        }

        return new Worker(filePath) as unknown as WorkerLike;
    } else if (CurrentRuntime === Runtime.Deno || CurrentRuntime === Runtime.Browser) {
        // Deno and Browser: Use standard Worker API
        return new Worker(url.href, {
            type: "module",
        }) as unknown as WorkerLike;
    } else {
        throw new Error(`Unsupported runtime: ${CurrentRuntime}`);
    }
}

/**
 * Get the default worker module URL for CSV parsing
 * This should point to the CSV worker implementation
 */
export function getDefaultWorkerModuleUrl(): URL {
    return new URL("../workers/csv_worker.ts", import.meta.url);
}
