
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

    // 2. Convert to IPC Stream (Uint8Array)
    const ipcBytes = tableToIPC(arrowTable, 'stream');

    // 3. Load into parquet-wasm Table
    const wasmTable = parquetWasm.Table.fromIPCStream(ipcBytes);

    // 4. Write to Parquet
    const parquetBytes = parquetWasm.writeParquet(wasmTable);
    console.log('Parquet bytes length:', parquetBytes.length);

    fs.writeFileSync('test_output.parquet', parquetBytes);

    // Clean up wasm memory
    wasmTable.free();
}

test().catch(console.error);
