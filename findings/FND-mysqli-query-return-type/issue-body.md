## Task

An application written against native `mysqli` runs its DDL/DML through `ZtdMysqli` in tests. The README says `ZtdMysqli` extends `mysqli` and can be passed wherever the application expects a mysqli connection.

## Expected

`mysqli::query()` returns `true` for successful statements that produce no result set (CREATE TABLE, INSERT, UPDATE, DELETE, DROP TABLE) and a `mysqli_result` only for SELECT-like statements ([PHP manual](https://www.php.net/manual/en/mysqli.query.php)). Application code such as `if ($mysqli->query($sql) === true)` or `is_bool($result)` therefore behaves the same under `ZtdMysqli`.

## Actual

`ZtdMysqli::query()` returns a `mysqli_result` object for CREATE TABLE, INSERT, UPDATE, DELETE and DROP TABLE. `=== true` checks fail; the object reports `num_rows` equal to the affected rows for INSERT/UPDATE/DELETE while `fetch_all()` returns an empty array. `lastAffectedRows()` is correct.

```
{"statement":"CREATE TABLE","native":{"type":"bool","value":true},"ztd":{"type":"mysqli_result","num_rows":0,"field_count":1,"fetch_all":[]},"same_type":false}
{"statement":"INSERT","native":{"type":"bool","value":true},"ztd":{"type":"mysqli_result","num_rows":2,"field_count":2,"fetch_all":[]},"same_type":false}
{"statement":"UPDATE","native":{"type":"bool","value":true},"ztd":{"type":"mysqli_result","num_rows":1,"field_count":3,"fetch_all":[]},"same_type":false}
{"statement":"DELETE","native":{"type":"bool","value":true},"ztd":{"type":"mysqli_result","num_rows":1,"field_count":2,"fetch_all":[]},"same_type":false}
{"statement":"SELECT","native":{"type":"mysqli_result",...},"ztd":{"type":"mysqli_result",...},"same_type":true}
{"statement":"DROP TABLE","native":{"type":"bool","value":true},"ztd":{"type":"mysqli_result","num_rows":0,"field_count":1,"fetch_all":[]},"same_type":false}
{"idiom":"$mysqli->query(ddl) === true","native_expected":true,"ztd_actual":false}
```

## Reproduction

Empty MySQL database, no physical tables. `composer require --dev k-kinzal/ztd-query-mysqli-adapter:dev-main k-kinzal/ztd-query-mysql:dev-main`, then:

```php
<?php
declare(strict_types=1);
require 'vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
[$host, $port, $db, $user, $password] = ['127.0.0.1', 3306, 'test', 'root', 'root'];

function describe(mixed $v): array
{
    $d = ['type' => get_debug_type($v)];
    if ($v instanceof mysqli_result) {
        $d += ['num_rows' => $v->num_rows, 'field_count' => $v->field_count, 'fetch_all' => $v->fetch_all(MYSQLI_ASSOC)];
    } else {
        $d['value'] = $v;
    }
    return $d;
}

function run(mysqli $conn, string $table): array
{
    $out = [];
    $out['CREATE TABLE'] = describe($conn->query("CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)"));
    $out['INSERT'] = describe($conn->query("INSERT INTO $table (id, name) VALUES (1, 'Alice'), (2, 'Bob')"));
    $out['UPDATE'] = describe($conn->query("UPDATE $table SET name = 'Robert' WHERE id = 2"));
    $out['DELETE'] = describe($conn->query("DELETE FROM $table WHERE id = 1"));
    $out['SELECT'] = describe($conn->query("SELECT id, name FROM $table ORDER BY id"));
    $out['DROP TABLE'] = describe($conn->query("DROP TABLE $table"));
    return $out;
}

$native = new mysqli($host, $user, $password, $db, $port);
$expected = run($native, 'repro_native');          // physical table, dropped by the script
$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$actual = run($ztd, 'repro_ztd');                  // session only

foreach ($expected as $statement => $exp) {
    echo json_encode(['statement' => $statement, 'native' => $exp, 'ztd' => $actual[$statement],
        'same_type' => $exp['type'] === $actual[$statement]['type']]), "\n";
}
var_dump($ztd->query('CREATE TABLE idiom (id INT PRIMARY KEY)') === true); // native: true, ZtdMysqli: false
```

```sh
php repro.php
```

## Versions

- Upstream `main` aac51e9d99121f79c98720fec3f8ddc03bf7c59b; split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- Also reproduced on the previous references (ztd-query-mysql bce93c331b03aef14fdd1ff5e7330424d7e0f6c9, ztd-query-mysqli-adapter 0faf44e71fbcd5065b6bb9a5b207b85e7e1aed7e, upstream 3a6c7e361a1613a7d75288a4628058e2b3af0e67), so this is not a recent regression.
- PHP 8.5.8 (macOS arm64) and PHP 8.1.34 (Linux aarch64), mysqlnd, MySQL 8.0.46 (`mysql:8.0` image), default `ZtdConfig`, `MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT`.

The full reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-query-return-type/`).
