<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\BasicCrudScenario;
use Tests\Support\AbstractPostgresPdoTestCase;

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
