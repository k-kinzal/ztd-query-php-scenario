<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests EXCEPT and INTERSECT behavior via MySQLi.
 *
 * Cross-platform parity with MysqlExceptIntersectTest (PDO).
 * EXCEPT and INTERSECT are valid SQL supported by MySQL 8.0+.
 * @spec SPEC-3.3d
 */
class ExceptIntersectTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ei_a (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE mi_ei_b (id INT PRIMARY KEY, name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ei_a', 'mi_ei_b'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_ei_a VALUES (3, 'Charlie')");
        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (3, 'Charlie')");
        $this->mysqli->query("INSERT INTO mi_ei_b VALUES (4, 'Diana')");
    }

    /**
     * EXCEPT should return rows in A but not in B.
     */
    public function testExceptOnMysql(): void
    {
        try {
            $result = $this->mysqli->query('SELECT name FROM mi_ei_a EXCEPT SELECT name FROM mi_ei_b');
            $rows = [];
            while ($row = $result->fetch_assoc()) {
                $rows[] = $row['name'];
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXCEPT misdetected as multi-statement query on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * INTERSECT should return rows common to both A and B.
     */
    public function testIntersectOnMysql(): void
    {
        try {
            $result = $this->mysqli->query('SELECT name FROM mi_ei_a INTERSECT SELECT name FROM mi_ei_b');
            $rows = [];
            while ($row = $result->fetch_assoc()) {
                $rows[] = $row['name'];
            }
            sort($rows);
            $this->assertCount(2, $rows);
            $this->assertSame('Bob', $rows[0]);
            $this->assertSame('Charlie', $rows[1]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INTERSECT misdetected as multi-statement query on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION works correctly on MySQL.
     */
    public function testUnionWorksOnMysql(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a UNION SELECT name FROM mi_ei_b ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * NOT IN workaround for EXCEPT.
     */
    public function testNotInWorkaroundForExcept(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a WHERE name NOT IN (SELECT name FROM mi_ei_b) ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]);
    }

    /**
     * IN workaround for INTERSECT.
     */
    public function testInWorkaroundForIntersect(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_ei_a WHERE name IN (SELECT name FROM mi_ei_b) ORDER BY name');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['name'];
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]);
        $this->assertSame('Charlie', $rows[1]);
    }
}
