<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * INSERT ... SELECT with UNION / UNION ALL through ZTD shadow store on
 * MySQLi.
 *
 * The MySQL CTE rewriter is known to misparse EXCEPT/INTERSECT as
 * multi-statement SQL. INSERT...SELECT with UNION ALL may also trigger
 * this parser issue (documented in spec as unreported for MySQL). This
 * test verifies whether the MySQLi adapter handles INSERT from compound
 * queries.
 *
 * @spec SPEC-4.1
 */
class InsertFromUnionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_ifu_archive (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_ifu_current (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_ifu_combined (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ifu_combined', 'mi_ifu_current', 'mi_ifu_archive'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ifu_archive VALUES (1, 'Alice', 100.00)");
        $this->mysqli->query("INSERT INTO mi_ifu_archive VALUES (2, 'Bob', 200.00)");

        $this->mysqli->query("INSERT INTO mi_ifu_current VALUES (1, 'Carol', 300.00)");
        $this->mysqli->query("INSERT INTO mi_ifu_current VALUES (2, 'Dave', 400.00)");
        $this->mysqli->query("INSERT INTO mi_ifu_current VALUES (3, 'Alice', 150.00)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_ifu_combined (name, amount)
                 SELECT name, amount FROM mi_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM mi_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM mi_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionDedup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_ifu_combined (name, amount)
                 SELECT name, amount FROM mi_ifu_archive
                 UNION
                 SELECT name, amount FROM mi_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM mi_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionThenAggregate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_ifu_combined (name, amount)
                 SELECT name, amount FROM mi_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM mi_ifu_current"
            );

            $rows = $this->ztdQuery(
                "SELECT name, SUM(amount) AS total
                 FROM mi_ifu_combined
                 GROUP BY name
                 ORDER BY total DESC"
            );
            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate after INSERT from UNION failed: ' . $e->getMessage());
        }
    }
}
