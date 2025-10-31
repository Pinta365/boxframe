/**
 * Worker pool abstraction for managing multiple workers
 * Provides round-robin job distribution, bounded in-flight queue, and error handling
 */

import type { WorkerLike } from "./create_worker.ts";
import { createWorker } from "./create_worker.ts";

/**
 * Job to be processed by a worker
 */
export interface WorkerJob {
    /** Sequence number for ordering */
    seq: number;
    /** Message payload to send to worker */
    payload: unknown;
    /** Transferable objects for zero-copy transfer */
    transfer?: Transferable[];
}

/**
 * Result from a worker
 */
export interface WorkerResult {
    /** Sequence number matching the job */
    seq: number;
    /** Result payload from worker */
    payload: unknown;
}

/**
 * Worker pool configuration
 */
export interface WorkerPoolOptions {
    /** Number of workers in the pool */
    size: number;
    /** Worker module URL */
    moduleUrl: string | URL;
    /** Maximum in-flight jobs (default: size * 2) */
    maxInflight?: number;
}

/**
 * Worker pool for distributing jobs across multiple workers
 */
export class WorkerPool {
    private workers: WorkerLike[] = [];
    private nextIndex = 0;
    private inflight = 0;
    private maxInflight: number;
    private isInitialized = false;
    private initPromise: Promise<void> | null = null;

    /** Callback invoked when a worker returns a result */
    onResult?: (result: WorkerResult) => void;
    /** Callback invoked when a worker encounters an error */
    onError?: (error: unknown, seq?: number) => void;

    constructor(private options: WorkerPoolOptions) {
        this.maxInflight = options.maxInflight ?? options.size * 2;
    }

    /**
     * Initialize the worker pool
     */
    async init(): Promise<void> {
        if (this.isInitialized) {
            return;
        }

        if (this.initPromise) {
            return this.initPromise;
        }

        this.initPromise = (async () => {
            this.workers = [];
            for (let i = 0; i < this.options.size; i++) {
                const worker = await createWorker(this.options.moduleUrl);

                worker.onmessage = (ev: MessageEvent) => {
                    const data = ev.data as { type?: string; seq?: number; message?: string };

                    // Handle ERROR messages from workers
                    if (data.type === "ERROR") {
                        this.inflight--;
                        const error = new Error(data.message || "Worker error");
                        this.onError?.(error, data.seq);
                        return;
                    }

                    this.inflight--;
                    const result: WorkerResult = {
                        seq: data.seq ?? -1,
                        payload: ev.data,
                    };
                    this.onResult?.(result);
                };

                worker.onerror = (ev: ErrorEvent) => {
                    this.onError?.(ev.error || ev.message, undefined);
                };

                this.workers.push(worker);
            }
            this.isInitialized = true;
        })();

        return this.initPromise;
    }

    /**
     * Broadcast a message to all workers
     */
    async broadcast(message: unknown): Promise<void> {
        await this.init();
        for (const worker of this.workers) {
            worker.postMessage(message);
        }
    }

    /**
     * Wait until there's capacity for a new job
     */
    async waitForCapacity(): Promise<void> {
        await this.init();
        while (this.inflight >= this.maxInflight) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
    }

    /**
     * Post a job to a worker (round-robin distribution)
     */
    async post(job: WorkerJob): Promise<void> {
        await this.init();

        await this.waitForCapacity();

        const worker = this.workers[this.nextIndex];
        this.nextIndex = (this.nextIndex + 1) % this.workers.length;
        this.inflight++;

        worker.postMessage(job.payload, job.transfer ?? []);
    }

    /**
     * Close all workers in the pool
     */
    async close(): Promise<void> {
        await this.init();

        for (const worker of this.workers) {
            try {
                worker.postMessage({ type: "CLOSE", payload: {} });
            } catch {
                // Worker may already be terminated
            }
        }

        const closePromises = this.workers.map((worker) => {
            if (worker.terminate) {
                return Promise.resolve(worker.terminate());
            }
            if (worker.close) {
                worker.close();
            }
            return Promise.resolve();
        });

        await Promise.all(closePromises);
        this.workers = [];
        this.isInitialized = false;
        this.initPromise = null;
    }
}
