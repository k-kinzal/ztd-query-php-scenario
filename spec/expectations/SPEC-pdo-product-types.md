# SPEC-pdo-product-types

## User task

An application saves product fixtures using PDO prepared INSERTs, then reads them
into strictly typed application code or JSON. Replacing PDO with ZtdPdo must
preserve the values and PHP types of these ordinary MySQL columns.

## Expected behavior and basis

- On clean, equivalent schemas, native PDO and ZtdPdo return identical rows using
  both `query()` and freshly prepared SELECTs, including exact PHP scalar types.
- Numeric strings supplied for INT/BIGINT/DOUBLE/DECIMAL are converted as native
  MySQL converts them. An integer supplied for VARCHAR becomes a string. Decimal
  input `99.999` for DECIMAL(10,2) becomes string `100.00`; NULL remains NULL.
- Test `ATTR_EMULATE_PREPARES` false/true and `ATTR_STRINGIFY_FETCHES` false/true,
  with execute-array parameters and explicit bindValue parameter types. With
  stringification disabled, INT/BIGINT/BOOLEAN are PHP integers, DOUBLE is a
  float, and DECIMAL/VARCHAR are strings on the selected 64-bit mysqlnd runtime.
  With stringification enabled, non-NULL values are strings. Switching this
  fetch setting after the write must affect reads without changing row values.
- All writes succeed. For ZTD, the physical table stays empty; for native PDO,
  physical rows contain the written fixtures. This deliberate isolation
  difference is excluded from the row comparison.

Basis: the adapter's public [README at the tested baseline](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-pdo-adapter/README.md)
describes a PDO replacement for application SQL and session-local writes.
The [MySQL specification](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-mysql/docs/spec.md#data-type-errors)
describes conversion of convertible input values.
The [PDO attributes manual](https://www.php.net/manual/en/pdo.setattribute.php)
defines stringification and emulation options; the
[execute manual](https://www.php.net/manual/en/pdostatement.execute.php) defines
array parameters. Clean native controls establish the exact driver results.

## Setup and operation order

Use a disposable MySQL database and a fresh table for each native/ZTD case.
Create the physical schema before wrapping the raw PDO with `fromPdo()`.
No seed rows. Columns, in INSERT order: id INT primary key, stock INT,
external_id BIGINT, weight DOUBLE, price DECIMAL(10,2), sku VARCHAR(40),
active BOOLEAN, note VARCHAR(40) nullable.

Insert one product with matching inputs and another with convertible inputs.
Read using query and prepare, toggle stringification and repeat both reads,
then inspect the physical rows through the raw connection. Compare exact
values/types with an independently initialized native case using the same
options and parameters. Drop only the scenario's table in cleanup.

## Scope and gaps

This expectation initially covers MySQL PDO with ERRMODE_EXCEPTION,
NULL_NATURAL, FETCH_ASSOC, and default ZTD configuration. All columns appear in
schema order to keep the existing column-order finding out of this scenario.
No claims about invalid input, overflow, unsigned BIGINT beyond PHP_INT_MAX,
alternate error modes, UPDATE, pre-existing physical rows, session-only DDL,
MySQLi, SQLite, PostgreSQL, or unexecuted PHP/database versions.

Legacy discovery: SPEC-12.4/12.5 and SPEC-13.1/13.4;
`tests/Pdo/MysqlTypeRoundtripTest.php`. Those tests cast most numeric fetched
values before comparing them, so they do not establish exact type preservation.
