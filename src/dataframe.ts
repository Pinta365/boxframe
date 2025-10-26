import type { ColumnData, DataFrameOptions, DataValue, GroupByOptions, Index, QueryCondition, QueryNode, RowData } from "./types.ts";
import { isWasmEngineEnabled } from "./types.ts";
import { Series } from "./series.ts";
import { WasmEngine } from "./wasm_engine.ts";
import { createDataFrameGroups, DataFrameGroupBy } from "./dataframe_groupby.ts";

/**
 * DataFrame - A 2D labeled data structure (table of data)
 * Immutable data structure representing a collection of Series with a common index
 */
export class DataFrame {
    public readonly columns: string[];
    public readonly index: Index[];
    public readonly data: Map<string, Series<any>>;
    public readonly shape: [number, number];

    constructor(data: Record<string, any[]> | Map<string, any[]>, options: DataFrameOptions = {}) {
        const dataMap = data instanceof Map ? data : new Map(Object.entries(data));

        this.columns = options.columns || Array.from(dataMap.keys());

        if (options.index) {
            this.index = options.index;
        } else {
            const firstColumn = dataMap.values().next().value;
            this.index = firstColumn ? Array.from({ length: firstColumn.length }, (_, i) => i) : [];
        }

        this.data = new Map();
        for (const [columnName, columnData] of dataMap) {
            if (columnData.length !== this.index.length) {
                throw new Error(`Column '${columnName}' length (${columnData.length}) must match index length (${this.index.length})`);
            }

            this.data.set(
                columnName,
                new Series(columnData, {
                    name: columnName,
                    index: this.index,
                }),
            );
        }

        this.shape = [this.index.length, this.columns.length];
    }

    /**
     * Get the number of rows
     */
    get length(): number {
        return this.shape[0];
    }

    /**
     * Get data types for each column
     */
    get dtypes(): Record<string, string> {
        const result: Record<string, string> = {};
        for (const [colName, series] of this.data) {
            result[colName] = series.dtype;
        }
        return result;
    }

    /**
     * Get all values as a 2D array (rows x columns)
     */
    get values(): DataValue[][] {
        const result: DataValue[][] = [];
        for (let i = 0; i < this.length; i++) {
            const row: DataValue[] = [];
            for (const colName of this.columns) {
                const series = this.data.get(colName);
                row.push(series ? series.values[i] : null);
            }
            result.push(row);
        }
        return result;
    }

    /**
     * Get a Series by column name
     */
    get(columnName: string): Series<any> {
        const series = this.data.get(columnName);
        if (!series) {
            throw new Error(`Column '${columnName}' not found`);
        }
        return series;
    }

    /**
     * Get a Series by column name (array access syntax)
     */
    [key: string]: any;

    /**
     * Get multiple columns as a new DataFrame
     */
    getColumns(columnNames: string[]): DataFrame {
        const newData = new Map<string, any[]>();

        for (const colName of columnNames) {
            if (!this.data.has(colName)) {
                throw new Error(`Column '${colName}' not found`);
            }
            newData.set(colName, this.data.get(colName)!.values);
        }

        return new DataFrame(newData, { index: this.index });
    }

    /**
     * Create a copy of the DataFrame with new data
     */
    private _copy(newData?: Map<string, Series<any>>, newIndex?: Index[], newColumns?: string[]): DataFrame {
        const dataMap = newData || this.data;
        const dataRecord: Record<string, any[]> = {};

        for (const [colName, series] of dataMap) {
            dataRecord[colName] = series.values;
        }

        return new DataFrame(dataRecord, {
            index: newIndex || this.index,
            columns: newColumns || this.columns,
        });
    }

    /**
     * Label-based indexing
     */
    loc(rowLabels: Index[], columnLabels?: string[]): DataFrame {
        const targetColumns = columnLabels || this.columns;
        const newData = new Map<string, any[]>();

        for (const colName of targetColumns) {
            const series = this.data.get(colName);
            if (!series) {
                throw new Error(`Column '${colName}' not found`);
            }

            const filteredSeries = series.loc(rowLabels);
            newData.set(colName, filteredSeries.values);
        }

        return new DataFrame(newData, {
            index: rowLabels,
            columns: targetColumns,
        });
    }

    /**
     * Integer-based indexing
     */
    iloc(rowPositions: number[], columnPositions?: number[]): DataFrame {
        const targetColumns = columnPositions ? columnPositions.map((pos) => this.columns[pos]) : this.columns;

        const newData = new Map<string, any[]>();

        for (const colName of targetColumns) {
            const series = this.data.get(colName);
            if (!series) {
                throw new Error(`Column '${colName}' not found`);
            }

            const filteredSeries = series.ilocRange(rowPositions);
            newData.set(colName, filteredSeries.values);
        }

        const newIndex = rowPositions.map((pos) => this.index[pos]);

        return new DataFrame(newData, {
            index: newIndex,
            columns: targetColumns,
        });
    }

    /**
     * Filter DataFrame based on boolean mask
     */
    filter(mask: boolean[]): DataFrame {
        if (mask.length !== this.length) {
            throw new Error(`Mask length (${mask.length}) must match DataFrame length (${this.length})`);
        }

        const newData = new Map<string, any[]>();
        const newIndex: Index[] = [];

        for (let i = 0; i < this.length; i++) {
            if (mask[i]) {
                newIndex.push(this.index[i]);
            }
        }

        for (const [colName, series] of this.data) {
            const values = series.values;
            const filteredValues: DataValue[] = [];

            for (let i = 0; i < this.length; i++) {
                if (mask[i]) {
                    filteredValues.push(values[i]);
                }
            }

            newData.set(colName, filteredValues);
        }

        return new DataFrame(newData, { index: newIndex });
    }

    /**
     * Query DataFrame using string expressions
     *
     * @param expression - Query expression string (e.g., 'age > 25', 'salary > 50000 and age < 30')
     * @returns Filtered DataFrame
     *
     * @example
     * ```typescript
     * const df = new DataFrame({
     *   name: ['Alice', 'Bob', 'Charlie'],
     *   age: [25, 30, 35],
     *   salary: [50000, 60000, 70000]
     * });
     *
     * // Simple condition
     * const young = df.query('age < 30');
     *
     * // Complex condition with AND/OR
     * const filtered = df.query('age > 25 and salary < 65000');
     *
     * // String matching
     * const alice = df.query('name == "Alice"');
     *
     * // Multiple conditions
     * const result = df.query('age >= 25 or salary > 55000');
     * ```
     */
    query(expression: string): DataFrame {
        try {
            const parsedQuery = this._parseQueryExpression(expression);

            const mask = this._evaluateQuery(parsedQuery);

            return this.filter(mask);
        } catch (error) {
            throw new Error(`Query evaluation failed: ${(error as Error).message}`);
        }
    }

    /**
     * Drop specified columns
     */
    dropColumns(columns: string[]): DataFrame {
        const newData = new Map<string, any[]>();
        const newColumns = this.columns.filter((col) => !columns.includes(col));

        for (const colName of newColumns) {
            newData.set(colName, this.data.get(colName)!.values);
        }

        return new DataFrame(newData, {
            index: this.index,
            columns: newColumns,
        });
    }

    /**
     * Drop specified rows by index labels
     */
    dropRows(labels: Index[]): DataFrame {
        const mask = this.index.map((idx) => !labels.includes(idx));
        return this.filter(mask);
    }

    /**
     * Drop specified rows by index positions
     */
    drop(positions: number[]): DataFrame {
        const mask = this.index.map((_, i) => !positions.includes(i));
        return this.filter(mask);
    }

    /**
     * Rename columns
     */
    rename(columns: { [oldName: string]: string }): DataFrame {
        const newData = new Map<string, any[]>();
        const newColumns: string[] = [];

        for (const oldColName of this.columns) {
            const newColName = columns[oldColName] || oldColName;
            newColumns.push(newColName);
            newData.set(newColName, this.data.get(oldColName)!.values);
        }

        return new DataFrame(newData, {
            index: this.index,
            columns: newColumns,
        });
    }

    /**
     * Add or modify columns
     */
    assign(assignments: { [columnName: string]: ((row: RowData) => DataValue) | ColumnData }): DataFrame {
        const newData = new Map<string, ColumnData>();

        for (const [colName, series] of this.data) {
            newData.set(colName, series.values);
        }

        for (const [colName, assignment] of Object.entries(assignments)) {
            if (typeof assignment === "function") {
                const columnValues = new Map<string, any[]>();
                for (const [existingCol, existingSeries] of this.data) {
                    columnValues.set(existingCol, existingSeries.values);
                }

                const newValues = this.index.map((_, i) => {
                    const row: RowData = {};
                    for (const [existingCol, values] of columnValues) {
                        row[existingCol] = values[i];
                    }
                    return assignment(row);
                });
                newData.set(colName, newValues);
            } else {
                if (assignment.length !== this.length) {
                    throw new Error(`Assignment for column '${colName}' length (${assignment.length}) must match DataFrame length (${this.length})`);
                }
                newData.set(colName, assignment);
            }
        }

        const newColumns = Array.from(newData.keys());

        return new DataFrame(newData, {
            index: this.index,
            columns: newColumns,
        });
    }

    /**
     * Sort by values in specified columns
     */
    sortValues(by: string | string[], ascending: boolean = true): DataFrame {
        const sortColumns = Array.isArray(by) ? by : [by];

        if (sortColumns.length === 1) {
            const colName = sortColumns[0];
            const series = this.data.get(colName);
            if (!series) {
                throw new Error(`Column '${colName}' not found`);
            }

            if (
                series.dtype === "float64" && series.values.every((v) => typeof v === "number" || v === null) &&
                isWasmEngineEnabled()
            ) {
                const values = series.values.map((v) => v === null ? NaN : v as number);
                const engine = WasmEngine.instance;
                const seriesId = engine.registerSeriesF64(new Float64Array(values));
                const indices = engine.sortIndicesF64(seriesId, ascending, true);
                engine.freeSeries(seriesId);
                return this._reorderByIndices(indices);
            }

            if (
                series.dtype === "int32" && series.values.every((v) => typeof v === "number" || v === null) && isWasmEngineEnabled()
            ) {
                const values = series.values.map((v) => v === null ? -2147483648 : v as number);
                const engine = WasmEngine.instance;
                const seriesId = engine.registerSeriesI32(new Int32Array(values));
                const indices = engine.sortIndicesI32(seriesId, ascending, true);
                engine.freeSeriesI32(seriesId);
                return this._reorderByIndices(indices);
            }

            const values = series.values;
            const indices = Array.from({ length: this.length }, (_, i) => i);

            indices.sort((a, b) => {
                const valA = values[a];
                const valB = values[b];

                if (valA === valB) return 0;
                if (valA === null || valA === undefined) return ascending ? 1 : -1;
                if (valB === null || valB === undefined) return ascending ? -1 : 1;

                const comparison = valA < valB ? -1 : valA > valB ? 1 : 0;
                return ascending ? comparison : -comparison;
            });

            return this._reorderByIndices(indices);
        }

        if (sortColumns.length === 2) {
            const series1 = this.data.get(sortColumns[0]);
            const series2 = this.data.get(sortColumns[1]);

            if (series1 && series2) {
                if (
                    series1.dtype === "float64" && series2.dtype === "float64" &&
                    series1.values.every((v) => typeof v === "number" || v === null) &&
                    series2.values.every((v) => typeof v === "number" || v === null) &&
                    isWasmEngineEnabled()
                ) {
                    const values1 = new Float64Array(series1.values.map((v) => v === null ? NaN : v as number));
                    const values2 = new Float64Array(series2.values.map((v) => v === null ? NaN : v as number));
                    const engine = WasmEngine.instance;
                    const id1 = engine.registerSeriesF64(values1);
                    const id2 = engine.registerSeriesF64(values2);
                    const indices = engine.sortTwoColumnsIndicesF64(id1, id2, ascending, ascending, true);
                    engine.freeSeries(id1);
                    engine.freeSeries(id2);
                    return this._reorderByIndices(indices);
                }

                if (
                    series1.dtype === "int32" && series2.dtype === "int32" &&
                    series1.values.every((v) => typeof v === "number" || v === null) &&
                    series2.values.every((v) => typeof v === "number" || v === null) &&
                    isWasmEngineEnabled()
                ) {
                    const values1 = new Int32Array(series1.values.map((v) => v === null ? -2147483648 : v as number));
                    const values2 = new Int32Array(series2.values.map((v) => v === null ? -2147483648 : v as number));
                    const engine = WasmEngine.instance;
                    const id1 = engine.registerSeriesI32(values1);
                    const id2 = engine.registerSeriesI32(values2);
                    const indices = engine.sortTwoColumnsIndicesI32(id1, id2, ascending, ascending, true);
                    engine.freeSeriesI32(id1);
                    engine.freeSeriesI32(id2);
                    return this._reorderByIndices(indices);
                }

                const values1 = series1.values;
                const values2 = series2.values;
                const indices = Array.from({ length: this.length }, (_, i) => i);

                indices.sort((a, b) => {
                    const valA1 = values1[a];
                    const valB1 = values1[b];

                    if (valA1 !== valB1) {
                        if (valA1 === null || valA1 === undefined) return ascending ? 1 : -1;
                        if (valB1 === null || valB1 === undefined) return ascending ? -1 : 1;

                        const comparison1 = valA1 < valB1 ? -1 : valA1 > valB1 ? 1 : 0;
                        const result1 = ascending ? comparison1 : -comparison1;
                        if (result1 !== 0) return result1;
                    }

                    const valA2 = values2[a];
                    const valB2 = values2[b];

                    if (valA2 === valB2) return 0;
                    if (valA2 === null || valA2 === undefined) return ascending ? 1 : -1;
                    if (valB2 === null || valB2 === undefined) return ascending ? -1 : 1;

                    const comparison2 = valA2 < valB2 ? -1 : valA2 > valB2 ? 1 : 0;
                    return ascending ? comparison2 : -comparison2;
                });

                return this._reorderByIndices(indices);
            }
        }

        const columnData = new Map<string, any[]>();
        for (const colName of sortColumns) {
            const series = this.data.get(colName);
            if (!series) {
                throw new Error(`Column '${colName}' not found`);
            }
            columnData.set(colName, series.values);
        }

        const sortKeys = this.index.map((_, i) => {
            const key: DataValue[] = [];
            for (const colName of sortColumns) {
                const values = columnData.get(colName)!;
                key.push(values[i]);
            }
            return { index: i, key };
        });

        sortKeys.sort((a, b) => {
            for (let i = 0; i < a.key.length; i++) {
                const valA = a.key[i];
                const valB = b.key[i];

                if (valA === valB) continue;
                if (valA === null || valA === undefined) return ascending ? 1 : -1;
                if (valB === null || valB === undefined) return ascending ? -1 : 1;

                const comparison = valA < valB ? -1 : valA > valB ? 1 : 0;
                const result = ascending ? comparison : -comparison;
                if (result !== 0) return result;
            }
            return 0;
        });

        const sortedIndices = sortKeys.map((item) => item.index);
        return this._reorderByIndices(sortedIndices);
    }

    /**
     * Helper method to reorder DataFrame by indices
     */
    private _reorderByIndices(indices: number[]): DataFrame {
        const newData = new Map<string, any[]>();
        const newIndex = indices.map((i) => this.index[i]);

        for (const [colName, series] of this.data) {
            const values = series.values;
            const newValues = new Array(indices.length);

            for (let i = 0; i < indices.length; i++) {
                newValues[i] = values[indices[i]];
            }

            newData.set(colName, newValues);
        }

        return new DataFrame(newData, {
            index: newIndex,
            columns: this.columns,
        });
    }

    /**
     * Sort by index
     */
    sortIndex(ascending: boolean = true): DataFrame {
        const indices = Array.from({ length: this.length }, (_, i) => i);

        indices.sort((a, b) => {
            const idxA = this.index[a];
            const idxB = this.index[b];

            const comparison = idxA < idxB ? -1 : idxA > idxB ? 1 : 0;
            return ascending ? comparison : -comparison;
        });

        return this._reorderByIndices(indices);
    }

    /**
     * Reset index to default integer range
     */
    resetIndex(): DataFrame {
        const newIndex = Array.from({ length: this.length }, (_, i) => i);
        return this._copy(undefined, newIndex);
    }

    /**
     * Calculate memory usage of the DataFrame
     */
    memoryUsage(deep: boolean = true): number {
        let totalBytes = 0;

        for (const [_columnName, series] of this.data) {
            if (series.dtype === "float64") {
                totalBytes += series.length * 8;
            } else if (series.dtype === "int32") {
                totalBytes += series.length * 4;
            } else if (series.dtype === "string") {
                if (deep) {
                    const stringValues = series.values as string[];
                    totalBytes += stringValues.reduce((sum, str) => sum + (str?.length || 0) * 2, 0);
                } else {
                    totalBytes += series.length * 8;
                }
            } else if (series.dtype === "bool") {
                totalBytes += series.length;
            } else {
                totalBytes += series.length * 8;
            }
            //add index memory
            totalBytes += series.index.length * 8;
        }

        // current WASM memory usage if available
        if (isWasmEngineEnabled()) {
            try {
                const wasmMemory = WasmEngine.getMemoryUsage();
                totalBytes += wasmMemory;
            } catch (_error) {
                // WASM memory query failed, continue without it
            }
        }

        return totalBytes;
    }

    /**
     * Drop duplicate rows
     */
    dropDuplicates(subset?: string[]): DataFrame {
        const targetColumns = subset || this.columns;
        const seen = new Set<string>();
        const mask: boolean[] = [];

        const columnValues = new Map<string, any[]>();
        for (const col of targetColumns) {
            const series = this.data.get(col);
            columnValues.set(col, series ? series.values : []);
        }

        for (let i = 0; i < this.length; i++) {
            const keyParts: string[] = [];
            for (const col of targetColumns) {
                const values = columnValues.get(col)!;
                keyParts.push(String(values[i]));
            }
            const key = keyParts.join("|");

            if (!seen.has(key)) {
                seen.add(key);
                mask.push(true);
            } else {
                mask.push(false);
            }
        }

        return this.filter(mask);
    }

    /**
     * Drop rows with null values
     */
    dropna(axis: "rows" | "columns" = "rows"): DataFrame {
        if (axis === "rows") {
            const columnValues = new Map<string, any[]>();
            for (const col of this.columns) {
                const series = this.data.get(col);
                columnValues.set(col, series ? series.values : []);
            }

            const mask: boolean[] = [];
            for (let i = 0; i < this.length; i++) {
                let hasNull = false;
                for (const col of this.columns) {
                    const values = columnValues.get(col)!;
                    if (values[i] === null || values[i] === undefined) {
                        hasNull = true;
                        break;
                    }
                }
                mask.push(!hasNull);
            }
            return this.filter(mask);
        } else {
            const validColumns = this.columns.filter((col) => {
                const series = this.data.get(col);
                if (!series) return false;

                const values = series.values;
                for (let i = 0; i < values.length; i++) {
                    if (values[i] === null || values[i] === undefined) {
                        return false;
                    }
                }
                return true;
            });
            return this.getColumns(validColumns);
        }
    }

    /**
     * Fill null values
     */
    fillna(value: DataValue | { [columnName: string]: DataValue }): DataFrame {
        const newData = new Map<string, ColumnData>();

        for (const [colName, series] of this.data) {
            const fillValue = typeof value === "object" && value !== null && !(value instanceof Date)
                ? (value as Record<string, DataValue>)[colName]
                : value;
            const filledSeries = series.fillna(fillValue);
            newData.set(colName, filledSeries.values);
        }

        return new DataFrame(newData, {
            index: this.index,
            columns: this.columns,
        });
    }

    /**
     * Check for null values
     */
    isnull(): DataFrame {
        const newData = new Map<string, any[]>();

        for (const [colName, series] of this.data) {
            const isnullSeries = series.isnull();
            newData.set(colName, isnullSeries.values);
        }

        return new DataFrame(newData, {
            index: this.index,
            columns: this.columns,
        });
    }

    /**
     * Check for non-null values
     */
    notnull(): DataFrame {
        const newData = new Map<string, any[]>();

        for (const [colName, series] of this.data) {
            const notnullSeries = series.notnull();
            newData.set(colName, notnullSeries.values);
        }

        return new DataFrame(newData, {
            index: this.index,
            columns: this.columns,
        });
    }

    /**
     * Get first n rows
     */
    head(n: number = 5): DataFrame {
        return this.iloc(Array.from({ length: Math.min(n, this.length) }, (_, i) => i));
    }

    /**
     * Get last n rows
     */
    tail(n: number = 5): DataFrame {
        const start = Math.max(0, this.length - n);
        return this.iloc(Array.from({ length: this.length - start }, (_, i) => start + i));
    }

    /**
     * Get DataFrame info
     */
    info(): string {
        const lines: string[] = [];
        lines.push(`DataFrame Info:`);
        lines.push(`Shape: ${this.shape[0]} rows × ${this.shape[1]} columns`);

        const indexInfo = this._getIndexInfo();
        lines.push(`Index: ${indexInfo}`);
        lines.push("");
        lines.push("Columns:");

        for (const colName of this.columns) {
            const series = this.data.get(colName);
            if (series) {
                const nonNullCount = series.notnull().values.filter(Boolean).length;
                lines.push(`  ${colName}: ${series.dtype} (${nonNullCount}/${this.length} non-null)`);
            }
        }

        return lines.join("\n");
    }

    /**
     * Get detailed index information
     */
    private _getIndexInfo(): string {
        if (this.index.length === 0) {
            return "Empty";
        }

        const isDefaultRange = this.index.every((val, i) => val === i);
        if (isDefaultRange) {
            return `RangeIndex(start=0, stop=${this.index.length}, step=1)`;
        }

        const isNumericRange = this.index.every((val, i) => typeof val === "number");
        if (isNumericRange && this.index.length > 1) {
            const step = (this.index[1] as number) - (this.index[0] as number);
            const isArithmeticProgression = this.index.every((val, i) => (val as number) === (this.index[0] as number) + i * step);
            if (isArithmeticProgression) {
                return `RangeIndex(start=${this.index[0]}, stop=${this.index[this.index.length - 1] + step}, step=${step})`;
            }
        }

        const maxShow = 3;
        const shown = this.index.slice(0, maxShow).join(", ");
        const truncated = this.index.length > maxShow ? "..." : "";
        return `${shown}${truncated}`;
    }

    /**
     * Get descriptive statistics
     */
    describe(): DataFrame {
        const numericColumns = this.columns.filter((col) => {
            const series = this.data.get(col);
            return series && ["int32", "float64"].includes(series.dtype);
        });

        if (numericColumns.length === 0) {
            throw new Error("No numeric columns found for describe()");
        }

        const stats = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"];
        const newData = new Map<string, any[]>();

        for (const stat of stats) {
            newData.set(stat, []);
        }

        for (const colName of numericColumns) {
            const series = this.data.get(colName)!;

            const count = series.count();
            const mean = series.mean();
            const std = series.std();
            const min = series.min();
            const max = series.max();

            if (count === 0) {
                newData.get("count")!.push(0);
                newData.get("mean")!.push(null);
                newData.get("std")!.push(null);
                newData.get("min")!.push(null);
                newData.get("25%")!.push(null);
                newData.get("50%")!.push(null);
                newData.get("75%")!.push(null);
                newData.get("max")!.push(null);
                continue;
            }

            const values = series.values.filter((v) => v !== null && v !== undefined) as number[];
            const sorted = [...values].sort((a, b) => a - b);

            newData.get("count")!.push(count);
            newData.get("mean")!.push(mean);
            newData.get("std")!.push(std);
            newData.get("min")!.push(min);
            newData.get("25%")!.push(sorted[Math.floor(count * 0.25)]);
            newData.get("50%")!.push(sorted[Math.floor(count * 0.5)]);
            newData.get("75%")!.push(sorted[Math.floor(count * 0.75)]);
            newData.get("max")!.push(max);
        }

        const dataRecord: Record<string, any[]> = {};
        for (const colName of numericColumns) {
            dataRecord[colName] = [];
        }

        for (let i = 0; i < stats.length; i++) {
            const stat = stats[i];
            const statValues = newData.get(stat)!;
            for (let j = 0; j < numericColumns.length; j++) {
                dataRecord[numericColumns[j]].push(statValues[j]);
            }
        }

        return new DataFrame(dataRecord, {
            index: stats,
            columns: numericColumns,
        });
    }

    /**
     * Convert to string representation
     */
    toString(): string {
        const lines: string[] = [];
        lines.push(`DataFrame`);
        lines.push(`shape: [${this.shape[0]}, ${this.shape[1]}]`);
        lines.push("");

        const maxRows = Math.min(10, this.length);
        const maxCols = Math.min(10, this.columns.length);

        const headerCols = this.columns.slice(0, maxCols);

        let maxRowLabelWidth = 0;
        for (let i = 0; i < maxRows; i++) {
            const rowLabel = String(this.index[i]);
            maxRowLabelWidth = Math.max(maxRowLabelWidth, rowLabel.length);
        }

        const columnWidths = headerCols.map((col) => {
            const series = this.data.get(col);
            if (!series) return col.length;

            let maxWidth = col.length;

            for (let i = 0; i < Math.min(maxRows, series.length); i++) {
                const value = this._formatValue(series.values[i]);
                maxWidth = Math.max(maxWidth, value.length);
            }

            return Math.min(maxWidth, 20);
        });

        const header = headerCols.map((col, i) => col.padEnd(columnWidths[i])).join(" ");
        lines.push(`${" ".repeat(maxRowLabelWidth + 1)}${header}`);

        for (let i = 0; i < maxRows; i++) {
            const rowLabel = String(this.index[i]);
            const rowData = headerCols.map((col, colIndex) => {
                const series = this.data.get(col);
                const value = series ? this._formatValue(series.values[i]) : "";
                return value.padEnd(columnWidths[colIndex]);
            }).join(" ");
            lines.push(`${rowLabel.padEnd(maxRowLabelWidth)} ${rowData}`);
        }

        if (this.length > maxRows) {
            lines.push("...");
        }

        return lines.join("\n");
    }

    /**
     * Format a value for display in toString()
     */
    private _formatValue(value: DataValue): string {
        if (value === null || value === undefined) {
            return "null";
        }

        if (typeof value === "number") {
            if (Math.abs(value) >= 1e6 || (Math.abs(value) < 1e-3 && value !== 0)) {
                return value.toExponential(3);
            }

            if (Number.isInteger(value)) {
                return value.toString();
            }

            const rounded = Math.round(value * 1e6) / 1e6;
            return rounded.toString();
        }

        if (typeof value === "string") {
            return value.length > 20 ? value.substring(0, 17) + "..." : value;
        }

        return String(value);
    }

    /**
     * Convert to JSON representation
     */
    toJSON(): { columns: string[]; index: Index[]; data: Record<string, any[]> } {
        const dataRecord: Record<string, any[]> = {};
        for (const [colName, series] of this.data) {
            dataRecord[colName] = series.values;
        }

        return {
            columns: this.columns,
            index: this.index,
            data: dataRecord,
        };
    }

    /**
     * Group DataFrame data for aggregation operations
     */
    groupBy(by: string | string[], options: GroupByOptions = {}): DataFrameGroupBy {
        const groupColumns = Array.isArray(by) ? by : [by];
        const { groups } = createDataFrameGroups(this, groupColumns, options);
        return new DataFrameGroupBy(this, groupColumns, groups, options);
    }

    /**
     * Parse query expression into a structured format
     * @private
     */
    private _parseQueryExpression(expression: string): QueryNode {
        const trimmed = expression.trim();

        if (trimmed.includes(" and ")) {
            const parts = this._splitByOperator(trimmed, " and ");
            if (parts.length === 2) {
                return {
                    type: "and",
                    left: this._parseQueryExpression(parts[0]),
                    right: this._parseQueryExpression(parts[1]),
                };
            }
        }

        if (trimmed.includes(" or ")) {
            const parts = this._splitByOperator(trimmed, " or ");
            if (parts.length === 2) {
                return {
                    type: "or",
                    left: this._parseQueryExpression(parts[0]),
                    right: this._parseQueryExpression(parts[1]),
                };
            }
        }

        return this._parseSimpleCondition(trimmed);
    }

    /**
     * Split expression by operator while respecting parentheses
     * @private
     */
    private _splitByOperator(expression: string, operator: string): string[] {
        let depth = 0;
        const start = 0;

        for (let i = 0; i < expression.length - operator.length + 1; i++) {
            if (expression[i] === "(") depth++;
            else if (expression[i] === ")") depth--;
            else if (depth === 0 && expression.substring(i, i + operator.length) === operator) {
                return [
                    expression.substring(start, i).trim(),
                    expression.substring(i + operator.length).trim(),
                ];
            }
        }

        return [expression];
    }

    /**
     * Parse a simple condition (e.g., 'age > 25', 'name == "Alice"')
     * @private
     */
    private _parseSimpleCondition(condition: string): QueryNode {
        if (condition.startsWith("(") && condition.endsWith(")")) {
            condition = condition.slice(1, -1).trim();
        }

        const operators = [">=", "<=", "==", "!=", ">", "<"];

        for (const op of operators) {
            const index = condition.indexOf(op);
            if (index !== -1) {
                const column = condition.substring(0, index).trim();
                const value = condition.substring(index + op.length).trim();

                if (value === "") {
                    throw new Error(`Invalid condition: missing value after operator '${op}'`);
                }

                if (!this.columns.includes(column)) {
                    throw new Error(`Column '${column}' not found in DataFrame`);
                }

                return {
                    type: "condition",
                    column,
                    operator: op as ">" | "<" | ">=" | "<=" | "==" | "!=",
                    value: this._parseValue(value),
                };
            }
        }

        throw new Error(`Invalid condition: ${condition}`);
    }

    /**
     * Parse string value, handling quotes and data types
     * @private
     */
    private _parseValue(valueStr: string): DataValue {
        if (
            (valueStr.startsWith('"') && valueStr.endsWith('"')) ||
            (valueStr.startsWith("'") && valueStr.endsWith("'"))
        ) {
            return valueStr.slice(1, -1);
        }

        if (valueStr === "null") {
            return null;
        }
        if (valueStr === "undefined") {
            return undefined;
        }
        if (valueStr === "true") {
            return true;
        }
        if (valueStr === "false") {
            return false;
        }

        const numValue = Number(valueStr);
        if (!isNaN(numValue)) {
            return numValue;
        }

        return valueStr;
    }

    /**
     * Evaluate parsed query against DataFrame rows
     * @private
     */
    private _evaluateQuery(node: QueryNode): boolean[] {
        if (node.type === "condition") {
            return this._evaluateCondition(node);
        }

        return this._evaluateNode(node);
    }

    /**
     * Recursive evaluation for complex queries
     * @private
     */
    private _evaluateNode(node: QueryNode): boolean[] {
        switch (node.type) {
            case "condition":
                return this._evaluateCondition(node);
            case "and": {
                const leftResult = this._evaluateNode(node.left);
                const rightResult = this._evaluateNode(node.right);
                return leftResult.map((left, i) => left && rightResult[i]);
            }
            case "or": {
                const leftResultOr = this._evaluateNode(node.left);
                const rightResultOr = this._evaluateNode(node.right);
                return leftResultOr.map((left, i) => left || rightResultOr[i]);
            }
            default:
                throw new Error(`Unknown node type: ${(node as any).type}`);
        }
    }

    /**
     * Condition evaluation using vectorized operations
     * @private
     */
    private _evaluateCondition(condition: QueryCondition): boolean[] {
        const series = this.data.get(condition.column);
        if (!series) {
            throw new Error(`Column '${condition.column}' not found`);
        }

        const values = series.values;
        const targetValue = condition.value;

        switch (condition.operator) {
            case ">":
                return values.map((v) => v != null && targetValue != null && v > targetValue);
            case "<":
                return values.map((v) => v != null && targetValue != null && v < targetValue);
            case ">=":
                return values.map((v) => v != null && targetValue != null && v >= targetValue);
            case "<=":
                return values.map((v) => v != null && targetValue != null && v <= targetValue);
            case "==":
                return values.map((v) => v === targetValue);
            case "!=":
                return values.map((v) => v !== targetValue);
            default:
                throw new Error(`Unknown operator: ${condition.operator}`);
        }
    }
}
