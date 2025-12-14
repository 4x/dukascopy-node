
import { parquetWrite } from 'hyparquet-writer';
import fs from 'fs';

try {
    const schema = {
        timestamp: { type: 'INT64' },
        price: { type: 'DOUBLE' }
    };

    // hyparquet-writer likely takes an object with schema and data?
    // Let's print the exports.
    console.log('Exports:', require('hyparquet-writer'));
} catch (e) {
    console.error(e);
}
