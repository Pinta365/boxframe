import type { DataArray, DataValue, DType, GroupByKey, GroupByOptions, Index, SeriesOptions } from "./types.ts";
import { isWasmEngineEnabled } from "./types.ts";
import { fromTypedArray, inferDType, toTypedArray } from "./types.ts";
import { createGroups, GroupBy } from "./groupby.ts";
import { WasmEngine } from "./wasm_engine.ts";

/**
 * Series - A 1D labeled array (single column of data)
 * Immutable data structure representing a single column with an associated index
 */
export class Series<T extends DataValue = DataValue> {
    public readonly data: DataArray<T>;
    public readonly dtype: DType;
    public readonly index: Index[];
    public readonly name: string;

    constructor(data: T[], options: SeriesOptions = {}) {
        this.name = options.name || "";
        this.index = options.index || data.map((_, i) => i);
        this.dtype = options.dtype || this._inferDTypeFromData(data);

        const hasNulls = data.some((val) => val === null || val === undefined);
        this.data = hasNulls ? data : toTypedArray(data, this.dtype);

        if (this.index.length !== data.length) {
            throw new Error(`Index length (${this.index.length}) must match data length (${data.length})`);
        }
    }

    /**
     * Get the length of the Series
     */
    get length(): number {
        return this.index.length;
    }

    /**
     * Get the shape as a tuple [length]
     */
    get shape(): [number] {
        return [this.length];
    }

    /**
     * Get values as a regular JavaScript array
     */
    get values(): T[] {
        return fromTypedArray(this.data, this.dtype);
    }

    /**
     * Create a copy of the Series with new data
     */
    private _copy(newData?: T[], newIndex?: Index[], newName?: string): Series<T> {
        return new Series(
            newData || this.values,
            {
                name: newName !== undefined ? newName : this.name,
                index: newIndex || this.index,
                dtype: this.dtype,
            },
        );
    }

    /**
     * Infer data type from the data array
     */
    private _inferDTypeFromData(data: T[]): DType {
        if (data.length === 0) return "string";

        let firstNonNullValue: T | null = null;
        let firstType: DType | null = null;

        for (let i = 0; i < data.length; i++) {
            const value = data[i];
            if (value !== null && value !== undefined) {
                if (firstNonNullValue === null) {
                    firstNonNullValue = value;
                    firstType = inferDType(value as DataValue);
                } else {
                    const currentType = inferDType(value as DataValue);
                    if (currentType !== firstType) {
                        return "string";
                    }
                }
            }
        }

        return firstType || "string";
    }

    /**
     * Get a value by index label
     */
    get(label: Index): T | undefined {
        const idx = this.index.indexOf(label);
        return idx !== -1 ? this.values[idx] : undefined;
    }

    /**
     * Get a value by integer position
     */
    iloc(position: number): T | undefined {
        return this.values[position];
    }

    /**
     * Get multiple values by index labels
     */
    loc(labels: Index[]): Series<T> {
        const indices = labels.map((label) => this.index.indexOf(label));
        const validIndices = indices.filter((idx) => idx !== -1);
        const newData = validIndices.map((idx) => this.values[idx]);
        const newIndex = validIndices.map((idx) => this.index[idx]);

        return this._copy(newData, newIndex);
    }

    /**
     * Get multiple values by integer positions
     */
    ilocRange(positions: number[]): Series<T> {
        const newData = positions.map((pos) => this.values[pos]).filter((val) => val !== undefined);
        const newIndex = positions.map((pos) => this.index[pos]).filter((idx) => idx !== undefined);

        return this._copy(newData, newIndex);
    }

    /**
     * Filter Series based on boolean mask
     */
    filter(mask: boolean[]): Series<T> {
        if (mask.length !== this.length) {
            throw new Error(`Mask length (${mask.length}) must match Series length (${this.length})`);
        }

        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const dataArray = new Float64Array(this.values as number[]);
            const seriesId = engine.registerSeriesF64(dataArray);
            const maskUint8 = new Uint8Array(mask.map((b) => b ? 1 : 0));
            const resultId = engine.filterF64(seriesId, maskUint8);
            engine.freeSeries(seriesId);
            const resultValues = engine.seriesToVecF64(resultId) as unknown as T[];
            engine.freeSeries(resultId);

            const newIndex: Index[] = [];
            for (let i = 0; i < this.length; i++) {
                if (mask[i]) newIndex.push(this.index[i]);
            }
            return this._copy(resultValues, newIndex);
        } else {
            let trueCount = 0;
            for (let i = 0; i < this.length; i++) {
                if (mask[i]) trueCount++;
            }

            if (trueCount === 0) {
                return this._copy([], []);
            }

            let newData: T[];
            let newIndex: Index[];

            if (this.dtype === "float64") {
                newData = new Float64Array(trueCount) as unknown as T[];
                newIndex = new Array(trueCount);
            } else {
                newData = new Array(trueCount);
                newIndex = new Array(trueCount);
            }

            const values = this.values;
            let count = 0;
            for (let i = 0; i < this.length; i++) {
                if (mask[i]) {
                    newData[count] = values[i];
                    newIndex[count] = this.index[i];
                    count++;
                }
            }

            return this._copy(newData, newIndex);
        }
    }

    /**
     * Drop values at specified index labels
     */
    drop(labels: Index[]): Series<T> {
        const mask = this.index.map((idx) => !labels.includes(idx));
        return this.filter(mask);
    }

    /**
     * Rename the Series
     */
    rename(name: string): Series<T> {
        return this._copy(undefined, undefined, name);
    }

    /**
     * Check if values are in a given set
     */
    isin(values: T[]): Series<boolean> {
        if (this.dtype === "float64" && values.every((v) => typeof v === "number") && isWasmEngineEnabled()) {
            try {
                const dataArray = new Float64Array(this.values as number[]);
                const valuesArray = new Float64Array(values as number[]);
                const rustResult = WasmEngine.isinF64(dataArray, valuesArray, 1e-9);
                const result = Array.from(rustResult).map((val) => val === 1);
                return new Series(result, {
                    name: `${this.name}_isin`,
                    index: this.index,
                    dtype: "bool",
                });
            } catch (error) {
                console.warn("Rust isin_f64 failed, falling back to JavaScript:", error);
            }
        }

        if (this.dtype === "int32" && values.every((v) => typeof v === "number" && Number.isInteger(v)) && isWasmEngineEnabled()) {
            try {
                const dataArray = new Int32Array(this.values as number[]);
                const valuesArray = new Int32Array(values as number[]);
                const rustResult = WasmEngine.isinI32(dataArray, valuesArray);
                const result = Array.from(rustResult).map((val) => val === 1);
                return new Series(result, {
                    name: `${this.name}_isin`,
                    index: this.index,
                    dtype: "bool",
                });
            } catch (error) {
                console.warn("Rust isin_i32 failed, falling back to JavaScript:", error);
            }
        }

        if (this.dtype === "string" && values.every((v) => typeof v === "string") && isWasmEngineEnabled()) {
            try {
                const rustResult = WasmEngine.isinString(this.values as string[], values as string[]);
                const result = Array.from(rustResult).map((val) => val === 1);
                return new Series(result, {
                    name: `${this.name}_isin`,
                    index: this.index,
                    dtype: "bool",
                });
            } catch (error) {
                console.warn("Rust isin_string failed, falling back to JavaScript:", error);
            }
        }

        const valuesSet = new Set(values);
        const result = this.values.map((val) => valuesSet.has(val));
        return new Series(result, {
            name: `${this.name}_isin`,
            index: this.index,
            dtype: "bool",
        });
    }

    /**
     * Check for null/undefined values
     */
    isnull(): Series<boolean> {
        const result = this.values.map((val) => val === null || val === undefined);
        return new Series(result, {
            name: `${this.name}_isnull`,
            index: this.index,
            dtype: "bool",
        });
    }

    /**
     * Check for non-null values
     */
    notnull(): Series<boolean> {
        const result = this.values.map((val) => val !== null && val !== undefined);
        return new Series(result, {
            name: `${this.name}_notnull`,
            index: this.index,
            dtype: "bool",
        });
    }

    /**
     * Drop null values
     */
    dropna(): Series<T> {
        const mask = this.notnull().values;
        return this.filter(mask);
    }

    /**
     * Fill null values with a specified value
     */
    fillna(value: T): Series<T> {
        const newData = this.values.map((val) => val === null || val === undefined ? value : val);
        return this._copy(newData);
    }

    /**
     * Get unique values
     */
    unique(): T[] {
        return [...new Set(this.values)];
    }

    /**
     * Get value counts
     */
    valueCounts(): Series<number> {
        const counts = new Map<T, number>();
        for (const value of this.values) {
            counts.set(value, (counts.get(value) || 0) + 1);
        }

        const uniqueValues = Array.from(counts.keys());
        const countValues = uniqueValues.map((val) => counts.get(val)!);

        return new Series(countValues, {
            name: `${this.name}_count`,
            index: uniqueValues.map(String),
            dtype: "int32",
        });
    }

    /**
     * Get number of unique values (excluding nulls by default)
     */
    nunique(dropna: boolean = true): number {
        if (dropna) {
            const nonNullValues = this.values.filter((val) => val !== null && val !== undefined);
            return new Set(nonNullValues).size;
        } else {
            return new Set(this.values).size;
        }
    }

    /**
     * Return boolean indicating whether all values in the Series are unique
     */
    isUnique(): boolean {
        return this.nunique(false) === this.length;
    }

    /**
     * Calculate sum of numeric values
     */
    sum(): number {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return 0;
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const value = engine.seriesSumF64(seriesId);
            engine.freeSeries(seriesId);
            return value;
        } else if (this.dtype === "int32") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return 0;
            return numericValues.reduce((sum, val) => sum + val, 0);
        } else if (this.dtype === "float64") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return 0;
            return numericValues.reduce((sum, val) => sum + val, 0);
        } else {
            return 0;
        }
    }

    /**
     * Calculate mean of numeric values
     */
    mean(): number {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const value = engine.seriesMeanF64(seriesId);
            engine.freeSeries(seriesId);
            return value;
        } else if (this.dtype === "int32") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
        } else if (this.dtype === "float64") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
        } else {
            return NaN;
        }
    }

    /**
     * Calculate standard deviation of numeric values
     */
    std(): number {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length <= 1) return NaN;
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const value = engine.seriesStdF64(seriesId);
            engine.freeSeries(seriesId);
            return value;
        } else if (this.dtype === "int32") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length <= 1) return NaN;
            const mean = numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
            const variance = numericValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / (numericValues.length - 1);
            return Math.sqrt(variance);
        } else if (this.dtype === "float64") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length <= 1) return NaN;
            const mean = numericValues.reduce((sum, val) => sum + val, 0) / numericValues.length;
            const variance = numericValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / (numericValues.length - 1);
            return Math.sqrt(variance);
        } else {
            return NaN;
        }
    }

    /**
     * Get minimum value
     */
    min(): number {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const value = engine.seriesMinF64(seriesId);
            engine.freeSeries(seriesId);
            return value;
        } else if (this.dtype === "int32") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((min, val) => val < min ? val : min, numericValues[0]);
        } else if (this.dtype === "float64") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((min, val) => val < min ? val : min, numericValues[0]);
        } else {
            return NaN;
        }
    }

    /**
     * Get maximum value
     */
    max(): number {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const value = engine.seriesMaxF64(seriesId);
            engine.freeSeries(seriesId);
            return value;
        } else if (this.dtype === "int32") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((max, val) => val > max ? val : max, numericValues[0]);
        } else if (this.dtype === "float64") {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            if (numericValues.length === 0) return NaN;
            return numericValues.reduce((max, val) => val > max ? val : max, numericValues[0]);
        } else {
            return NaN;
        }
    }

    /**
     * Count non-null values
     */
    count(): number {
        // Early testing shows JavaScript filter is more efficient than WASM (overhead is significant)
        return this.values.filter((v) => v !== null && v !== undefined).length;
    }

    /**
     * Sort by values
     */
    sortValues(ascending: boolean = true): Series<T> {
        if (this.dtype === "float64" && isWasmEngineEnabled()) {
            const engine = WasmEngine.instance;
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            const nullValues = this.values.filter((v) => v === null || v === undefined);
            if (numericValues.length === 0) return this._copy();
            const seriesId = engine.registerSeriesF64(new Float64Array(numericValues));
            const resultId = engine.sortValuesF64(seriesId, ascending, true);
            engine.freeSeries(seriesId);
            const sortedNumeric = engine.seriesToVecF64(resultId) as unknown as T[];
            engine.freeSeries(resultId);
            const newData = [...sortedNumeric, ...nullValues] as T[];
            const newIndex = this.index;
            return this._copy(newData, newIndex);
        } else if (this.dtype === "int32" && isWasmEngineEnabled()) {
            const numericValues = this.values.filter((v) => v !== null && v !== undefined) as number[];
            const nullValues = this.values.filter((v) => v === null || v === undefined);

            if (numericValues.length === 0) {
                return this._copy();
            }

            const engine = WasmEngine.instance;
            const seriesId = engine.registerSeriesI32(new Int32Array(numericValues));
            try {
                const indices = engine.sortIndicesI32(seriesId, ascending, true);
                const sortedNumeric = indices.map((i: number) => numericValues[i]);

                const newData = [...sortedNumeric, ...nullValues] as T[];
                const newIndex = this.index;

                return this._copy(newData, newIndex);
            } finally {
                engine.freeSeriesI32(seriesId);
            }
        } else {
            const values = this.values;
            const indices = Array.from({ length: this.length }, (_, i) => i);

            indices.sort((a, b) => {
                const valA = values[a];
                const valB = values[b];

                if (valA === valB) return 0;
                if (valA === null || valA === undefined) return 1;
                if (valB === null || valB === undefined) return -1;

                const comparison = valA < valB ? -1 : valA > valB ? 1 : 0;
                return ascending ? comparison : -comparison;
            });

            const newData = indices.map((i) => values[i]);
            const newIndex = indices.map((i) => this.index[i]);

            return this._copy(newData, newIndex);
        }
    }

    /**
     * Sort by index
     */
    sortIndex(ascending: boolean = true): Series<T> {
        const indices = Array.from({ length: this.length }, (_, i) => i);

        indices.sort((a, b) => {
            const idxA = this.index[a];
            const idxB = this.index[b];

            const comparison = idxA < idxB ? -1 : idxA > idxB ? 1 : 0;
            return ascending ? comparison : -comparison;
        });

        const newData = indices.map((i) => this.values[i]);
        const newIndex = indices.map((i) => this.index[i]);

        return this._copy(newData, newIndex);
    }

    /**
     * Reset index to default integer range
     */
    resetIndex(): Series<T> {
        const newIndex = Array.from({ length: this.length }, (_, i) => i);
        return this._copy(undefined, newIndex);
    }

    /**
     * Get first n values
     */
    head(n: number = 5): Series<T> {
        const newData = this.values.slice(0, n);
        const newIndex = this.index.slice(0, n);
        return this._copy(newData, newIndex);
    }

    /**
     * Get last n values
     */
    tail(n: number = 5): Series<T> {
        const newData = this.values.slice(-n);
        const newIndex = this.index.slice(-n);
        return this._copy(newData, newIndex);
    }

    /**
     * Convert to string representation
     */
    toString(): string {
        const lines: string[] = [];
        lines.push(`Series: ${this.name || "Unnamed"}`);
        lines.push(`dtype: ${this.dtype}`);
        lines.push(`length: ${this.length}`);
        lines.push("");

        const displayCount = Math.min(10, this.length);
        for (let i = 0; i < displayCount; i++) {
            const idx = this.index[i];
            const val = this.values[i];
            lines.push(`${idx}    ${val}`);
        }

        if (this.length > displayCount) {
            lines.push("...");
        }

        return lines.join("\n");
    }

    /**
     * Convert to JSON representation
     */
    toJSON(): { name: string; dtype: DType; index: Index[]; data: T[] } {
        return {
            name: this.name,
            dtype: this.dtype,
            index: this.index,
            data: this.values,
        };
    }

    /**
     * Group Series data for aggregation operations
     */
    groupBy(by?: GroupByKey, options: GroupByOptions = {}): GroupBy<T> {
        const { groups, groupKeys, fullGroupKeys } = createGroups(this, by, options);
        return new GroupBy(this, groupKeys, groups, fullGroupKeys, options);
    }
}
