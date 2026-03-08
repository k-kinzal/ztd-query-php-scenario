<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\BasicCrudScenario;
use Tests\Support\AbstractSqlitePdoTestCase;

/** @spec SPEC-1.1, SPEC-1.2, SPEC-2.1, SPEC-2.2, SPEC-2.3, SPEC-3.1, SPEC-4.1, SPEC-4.2, SPEC-4.3 */
class SqliteBasicCrudTest extends AbstractSqlitePdoTestCase
{
    use BasicCrudScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['users'];
    }
}
