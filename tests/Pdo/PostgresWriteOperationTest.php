<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\WriteOperationScenario;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-4.1, SPEC-4.2 */
class PostgresWriteOperationTest extends AbstractPostgresPdoTestCase
{
    use WriteOperationScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), price INT)';
    }

    protected function getTableNames(): array
    {
        return ['products'];
    }
}
