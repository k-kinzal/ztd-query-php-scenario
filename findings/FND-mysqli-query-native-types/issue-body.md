## Task

An application reads rows with `$mysqli->query($sql)->fetch_assoc()` and compares or serializes the values (strict comparisons, `json_encode`, DTO hydration). Tests run the same code through `ZtdMysqli`, which the README describes as a drop-in `mysqli`.

## Expected

With default connection options, native `mysqli::query()` results return every column as a PHP `string` (text protocol). Integers and floats are returned as native types only when `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` is set, or through prepared statements and `get_result()`. `ZtdMysqli::query()` is expected to return the same PHP types as the underlying `mysqli` for the same connection options, so tests exercise the same values as production.

## Actual

`ZtdMysqli::query()` with default options returns `int`/`float` for INT, BIGINT, DOUBLE and BOOLEAN columns, as if `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` were enabled. DECIMAL and VARCHAR stay strings. The prepared `get_result()` path matches native.

```
{"native default":{"query()":{"id":{"type":"string","value":"1"},"qty":{"type":"string","value":"9000000001"},"ratio":{"type":"string","value":"1.5"},"price":{"type":"string","value":"12.50"},"name":{"type":"string","value":"Alice"},"flag":{"type":"string","value":"1"}}, ...}}
{"native MYSQLI_OPT_INT_AND_FLOAT_NATIVE":{"query()":{"id":{"type":"int","value":1},"qty":{"type":"int","value":9000000001},"ratio":{"type":"float","value":1.5},"price":{"type":"string","value":"12.50"},"name":{"type":"string","value":"Alice"},"flag":{"type":"int","value":1}}, ...}}
{"ZtdMysqli default":{"query()":{"id":{"type":"int","value":1},"qty":{"type":"int","value":9000000001},"ratio":{"type":"float","value":1.5},"price":{"type":"string","value":"12.50"},"name":{"type":"string","value":"Alice"},"flag":{"type":"int","value":1}}, ...}}
{"query() matches native default":false,"prepared matches native":true}
```

Consequences: `$row['id'] === '1'` is true in production and false under ZTD; `json_encode($row)` yields `{"id":"1"}` in production and `{"id":1}` in tests; strict-typed hydration (`string $id`) throws under ZTD with `declare(strict_types=1)`.

## Reproduction

Empty MySQL database. `composer require --dev k-kinzal/ztd-query-mysqli-adapter:dev-main k-kinzal/ztd-query-mysql:dev-main`, then:

```php
<?php
declare(strict_types=1);
require 'vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
[$host, $port, $db, $user, $password] = ['127.0.0.1', 3306, 'test', 'root', 'root'];

function typed(array $row): array
{
    return array_map(static fn ($v): array => ['type' => get_debug_type($v), 'value' => $v], $row);
}

function observe(mysqli $conn, string $table): array
{
    $conn->query("CREATE TABLE $table (id INT PRIMARY KEY, qty BIGINT, ratio DOUBLE, price DECIMAL(10,2), name VARCHAR(20), flag BOOLEAN)");
    $conn->query("INSERT INTO $table VALUES (1, 9000000001, 1.5, 12.50, 'Alice', TRUE)");
    $sql = "SELECT id, qty, ratio, price, name, flag FROM $table";
    $plain = typed($conn->query($sql)->fetch_assoc());
    $stmt = $conn->prepare($sql);
    $stmt->execute();
    return ['query()' => $plain, 'prepare()->get_result()' => typed($stmt->get_result()->fetch_assoc())];
}

$native = new mysqli($host, $user, $password, $db, $port);
$nativeDefault = observe($native, 'repro_native');
$native->query('DROP TABLE repro_native');

$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$ztdDefault = observe($ztd, 'repro_ztd'); // session only, no physical table

echo json_encode(['native default' => $nativeDefault]), "\n";
echo json_encode(['ZtdMysqli default' => $ztdDefault]), "\n";
var_dump($nativeDefault['query()'] === $ztdDefault['query()']); // false
```

```sh
php repro.php
```

## Versions

- Upstream `main` aac51e9d99121f79c98720fec3f8ddc03bf7c59b; split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- Also reproduced on the previous references (ztd-query-mysql bce93c331b03aef14fdd1ff5e7330424d7e0f6c9, ztd-query-mysqli-adapter 0faf44e71fbcd5065b6bb9a5b207b85e7e1aed7e, upstream 3a6c7e361a1613a7d75288a4628058e2b3af0e67).
- PHP 8.5.8 (macOS arm64) and PHP 8.1.34 (Linux aarch64), mysqlnd, MySQL 8.0.46 (`mysql:8.0` image), default `ZtdConfig`.

If returning native types is intentional, documenting it in the mysqli adapter README would let users set `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` in production or adjust expectations. The full reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-query-native-types/`).
