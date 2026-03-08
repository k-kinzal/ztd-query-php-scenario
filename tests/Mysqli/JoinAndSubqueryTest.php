<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Scenarios\JoinAndSubqueryScenario;
use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests shared JOIN and subquery scenarios on MySQL via MySQLi.
 * @spec SPEC-3.3
 */
class JoinAndSubqueryTest extends AbstractMysqliTestCase
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
