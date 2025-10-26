/**
 * Shared utilities for grouping operations
 * Contains common calculation functions used by both Series and DataFrame grouping
 */

import type { DataValue } from "./types.ts";

/**
 * Calculate sum of values
 */
export function calculateSum(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length === 0) return 0;
    return numericValues.reduce((sum, val) => sum + val, 0);
}

/**
 * Calculate sum of values at specific indices
 */
export function calculateSumByIndices(values: DataValue[], indices: number[]): number {
    let sum = 0;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            sum += val;
        }
    }
    return sum;
}

/**
 * Calculate mean of values
 */
export function calculateMean(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length === 0) return NaN;
    return numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
}

/**
 * Calculate mean of values at specific indices
 */
export function calculateMeanByIndices(values: DataValue[], indices: number[]): number {
    let sum = 0;
    let count = 0;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            sum += val;
            count++;
        }
    }
    return count === 0 ? NaN : sum / count;
}

/**
 * Calculate count of non-null values
 */
export function calculateCount(values: DataValue[]): number {
    return values.filter((v) => v !== null && v !== undefined).length;
}

/**
 * Calculate count of non-null values at specific indices
 */
export function calculateCountByIndices(values: DataValue[], indices: number[]): number {
    let count = 0;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined) {
            count++;
        }
    }
    return count;
}

/**
 * Calculate minimum value
 */
export function calculateMin(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length === 0) return NaN;
    return Math.min(...numericValues);
}

/**
 * Calculate minimum value at specific indices
 */
export function calculateMinByIndices(values: DataValue[], indices: number[]): number {
    let min = Infinity;
    let hasValue = false;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            if (val < min) min = val;
            hasValue = true;
        }
    }
    return hasValue ? min : NaN;
}

/**
 * Calculate maximum value
 */
export function calculateMax(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length === 0) return NaN;
    return Math.max(...numericValues);
}

/**
 * Calculate maximum value at specific indices
 */
export function calculateMaxByIndices(values: DataValue[], indices: number[]): number {
    let max = -Infinity;
    let hasValue = false;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            if (val > max) max = val;
            hasValue = true;
        }
    }
    return hasValue ? max : NaN;
}

/**
 * Calculate standard deviation
 */
export function calculateStd(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length <= 1) return NaN;

    const mean = numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
    const variance = numericValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / (numericValues.length - 1);
    return Math.sqrt(variance);
}

/**
 * Calculate standard deviation at specific indices
 */
export function calculateStdByIndices(values: DataValue[], indices: number[]): number {
    let sum = 0;
    let count = 0;

    // First pass: calculate mean
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            sum += val;
            count++;
        }
    }

    if (count <= 1) return NaN;
    const mean = sum / count;

    // Second pass: calculate variance
    let variance = 0;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            variance += Math.pow(val - mean, 2);
        }
    }

    return Math.sqrt(variance / (count - 1));
}

/**
 * Calculate variance
 */
export function calculateVar(values: DataValue[]): number {
    const numericValues = values.filter((v) => v !== null && v !== undefined && typeof v === "number");
    if (numericValues.length <= 1) return NaN;

    const mean = numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
    return numericValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / (numericValues.length - 1);
}

/**
 * Calculate variance at specific indices
 */
export function calculateVarByIndices(values: DataValue[], indices: number[]): number {
    let sum = 0;
    let count = 0;

    // First pass: calculate mean
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            sum += val;
            count++;
        }
    }

    if (count <= 1) return NaN;
    const mean = sum / count;

    // Second pass: calculate variance
    let variance = 0;
    for (let i = 0; i < indices.length; i++) {
        const val = values[indices[i]];
        if (val !== null && val !== undefined && typeof val === "number") {
            variance += Math.pow(val - mean, 2);
        }
    }

    return variance / (count - 1);
}
