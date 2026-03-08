<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\JoinAndSubqueryScenario;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests shared JOIN and subquery scenarios on MySQL via PDO.
 * @spec SPEC-3.3
 */
class MysqlJoinAndSubqueryTest extends AbstractMysqlPdoTestCase
{
    use JoinAndSubqueryScenario;

    protected function getTableDDL(): array
    {
        return [
            'CREATE TABLE departments (id INT PRIMARY KEY, dept_name VARCHAR(255))',
            'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['users', 'departments'];
    }
}
