<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\WriteOperationScenario;
use Tests\Support\AbstractSqlitePdoTestCase;

/** @spec SPEC-4.1, SPEC-4.2 */
class SqliteWriteOperationTest extends AbstractSqlitePdoTestCase
{
    use WriteOperationScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['products'];
    }
}
