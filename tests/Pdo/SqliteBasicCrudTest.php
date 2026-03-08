<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\BasicCrudScenario;
use Tests\Support\AbstractSqlitePdoTestCase;

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
