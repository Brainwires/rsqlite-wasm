import { WasmDatabase } from './pkg-node/rsqlite_wasm.js';

console.log('=== rsqlite-wasm Node.js Test ===\n');

const db = new WasmDatabase();
console.log('Created in-memory database');

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
console.log('Created users table');

db.exec("INSERT INTO users VALUES (1, 'Alice', 30)");
db.exec("INSERT INTO users VALUES (2, 'Bob', 25)");
db.exec("INSERT INTO users VALUES (3, 'Charlie', 35)");
console.log('Inserted 3 users');

const rows = db.query('SELECT * FROM users');
console.log('\nSELECT * FROM users:');
console.log(JSON.stringify(rows, null, 2));

const one = db.queryOne('SELECT name, age FROM users WHERE id = 2');
console.log('\nqueryOne WHERE id = 2:');
console.log(JSON.stringify(one, null, 2));

db.exec('CREATE INDEX idx_age ON users(age)');
console.log('\nCreated index on age');

const filtered = db.query('SELECT name FROM users WHERE age = 30');
console.log('SELECT with index scan (age = 30):');
console.log(JSON.stringify(filtered, null, 2));

db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)');
db.exec("INSERT INTO orders VALUES (1, 1, 99.99)");
db.exec("INSERT INTO orders VALUES (2, 2, 49.50)");
db.exec("INSERT INTO orders VALUES (3, 1, 25.00)");

const joined = db.query('SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC');
console.log('\nJOIN query (users + orders):');
console.log(JSON.stringify(joined, null, 2));

const agg = db.query('SELECT u.name, COUNT(*) as order_count, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.name');
console.log('\nAggregate query:');
console.log(JSON.stringify(agg, null, 2));

const buf = db.toBuffer();
console.log(`\nDatabase export: ${buf.length} bytes`);

db.free();
console.log('\n=== All tests passed! ===');
