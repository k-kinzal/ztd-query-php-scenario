<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UNION queries with mutations on MySQLi.
 * @spec SPEC-3.3d
 */
class UnionMutationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_um_employees (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))',
            'CREATE TABLE mi_um_contractors (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_um_contractors', 'mi_um_employees'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_um_employees (id, name, dept) VALUES (1, 'Alice', 'Eng')");
        $this->mysqli->query("INSERT INTO mi_um_employees (id, name, dept) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (1, 'Dave', 'Eng')");
    }

    public function testUnionReflectsInserts(): void
    {
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (2, 'Frank', 'Eng')");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees WHERE dept = 'Eng'
            UNION ALL
            SELECT name FROM mi_um_contractors WHERE dept = 'Eng'
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Alice', 'Dave', 'Frank'], $names);
    }

    public function testUnionReflectsDeletes(): void
    {
        $this->mysqli->query("DELETE FROM mi_um_employees WHERE name = 'Alice'");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees
            UNION ALL
            SELECT name FROM mi_um_contractors
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Bob', 'Dave'], $names);
    }

    public function testUnionDistinct(): void
    {
        $this->mysqli->query("INSERT INTO mi_um_contractors (id, name, dept) VALUES (2, 'Alice', 'Eng')");

        $result = $this->mysqli->query("
            SELECT name FROM mi_um_employees
            UNION
            SELECT name FROM mi_um_contractors
            ORDER BY name
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Alice', 'Bob', 'Dave'], $names);
    }
}
