
import { Table, vectorFromArray, tableToIPC } from 'apache-arrow';
import * as parquetWasm from 'parquet-wasm';
import fs from 'fs';

async function test() {
    // 1. Create Arrow Table (Apache Arrow)
    const timestamps = new BigInt64Array([1000n, 2000n]);
    const prices = new Float64Array([1.1, 2.2]);

    const arrowTable = new Table({
        timestamp: vectorFromArray(timestamps),
        price: vectorFromArray(prices)
    });

    // 2. Convert to IPC (Uint8Array)
    const ipcBytes = tableToIPC(arrowTable, 'stream'); // Parquet wasm usually reads stream format too?

    // 3. Load into parquet-wasm Table
    // "Table.fromIPC" isn't directly exposed on class maybe?
    // "readParquet" reads parquet.
    // "Table.load" or similar?
    // Let's try `parquetWasm.Table.fromIPCStream` or similar if exists.
    // Or maybe `writeParquet` accepts Uint8Array in newer versions?
    // The previous error `expected instance of Table` implies `writeParquet` arguments strictly checked.

    // Wait, if I have IPC bytes, I want to WRITE parquet.
    // `writeParquet` takes a `Table` and returns `Uint8Array` (parquet bytes).
    // So I must convert my Arrow bytes to `parquetWasm.Table`.

    // `parquetWasm.Table`?
    // There is no `Table.fromIPC` static method?

    // Docs say:
    // const table = Table.fromIPCStream(ipcBuffer);

    // Let's try loading table from IPC.
}

// Inspect Table static methods
console.log(Object.getOwnPropertyNames(parquetWasm.Table));
