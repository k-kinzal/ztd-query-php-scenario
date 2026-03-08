<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\JoinAndSubqueryScenario;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shared JOIN and subquery scenarios on SQLite via PDO.
 * @spec pending
 */
class SqliteJoinAndSubqueryTest extends AbstractSqlitePdoTestCase
{
    use JoinAndSubqueryScenario;

    protected function getTableDDL(): array
    {
        return [
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)',
            'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['users', 'departments'];
    }
}
