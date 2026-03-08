<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\BasicCrudScenario;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-1.1, SPEC-1.2, SPEC-2.1, SPEC-2.2, SPEC-2.3, SPEC-3.1, SPEC-4.1, SPEC-4.2, SPEC-4.3 */
class PostgresBasicCrudTest extends AbstractPostgresPdoTestCase
{
    use BasicCrudScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['users'];
    }
}
