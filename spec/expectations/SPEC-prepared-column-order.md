# SPEC-prepared-column-order

## User task

An application builds an INSERT column list independently of DDL order, binds its values positionally, then fetches the inserted fixture by primary key.

## Expectation

Given `items (id INT PRIMARY KEY, name TEXT, price REAL, category TEXT)`, executing `INSERT INTO items (price, id, name) VALUES (?, ?, ?)` with `[19.99, 10, 'PrepItem']` returns true. Both an unfiltered read and a read for `id = 10` return exactly `[{"id":10,"name":"PrepItem","price":19.99,"category":null}]`. Isolated writes leave the physical table empty.

## Basis

The [PDO execute contract and positional example](https://www.php.net/manual/en/pdostatement.execute.php) associate values with placeholders in statement order. A clean native PDO control establishes SQL and fetched PHP types. The [ZTD PDO contract](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-pdo-adapter/README.md) promises session writes and later reads without physical changes. Physical effects deliberately differ between the native and ZTD controls.

## Scope and links

- [SQLite PDO scenario](../../scenarios/parameters/SCN-sqlite-insert-column-order/scenario.json), default ZTD options; other adapters and binding methods remain unverified.
- [Finding](../../findings/FND-sqlite-insert-column-order/README.md) holds observations and upstream disposition.
- Historical expectation: `SPEC-4.1`.
