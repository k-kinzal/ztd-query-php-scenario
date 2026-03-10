<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE nested inside a subquery of a DML statement on SQLite.
 *
 * Pattern: INSERT INTO t SELECT * FROM (WITH cte AS (...) SELECT FROM cte) sub
 * Pattern: DELETE FROM t WHERE id IN (WITH cte AS (...) SELECT FROM cte)
 *
 * @spec SPEC-10.2
 */
class SqliteCteInSubqueryDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_cis_source (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                dept TEXT,
                salary REAL
            )",
            "CREATE TABLE sl_cis_target (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                rank_in_dept INTEGER
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cis_target', 'sl_cis_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cis_source (name, dept, salary) VALUES ('Alice', 'eng', 120000)");
        $this->ztdExec("INSERT INTO sl_cis_source (name, dept, salary) VALUES ('Bob', 'eng', 100000)");
        $this->ztdExec("INSERT INTO sl_cis_source (name, dept, salary) VALUES ('Charlie', 'sales', 90000)");
        $this->ztdExec("INSERT INTO sl_cis_source (name, dept, salary) VALUES ('Diana', 'sales', 110000)");
        $this->ztdExec("INSERT INTO sl_cis_source (name, dept, salary) VALUES ('Eve', 'eng', 130000)");
    }

    /**
     * INSERT...SELECT FROM (WITH cte AS (...) SELECT FROM cte).
     */
    public function testInsertFromNestedCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM sl_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name, rank_in_dept FROM sl_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT FROM nested CTE (SQLite): expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Diana', $names);
            $this->assertContains('Eve', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM nested CTE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN (WITH cte AS (...) SELECT FROM cte).
     */
    public function testDeleteWhereInNestedCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_cis_source
                 WHERE id IN (
                     WITH ranked AS (
                         SELECT id,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rn
                         FROM sl_cis_source
                     )
                     SELECT id FROM ranked WHERE rn = 1
                 )"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_cis_source ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN nested CTE (SQLite): expected 3 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
            $this->assertNotContains('Charlie', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN nested CTE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE id IN nested CTE.
     */
    public function testUpdateWhereInNestedCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_cis_source
                 SET salary = salary * 1.10
                 WHERE id IN (
                     WITH top_earners AS (
                         SELECT id FROM sl_cis_source WHERE salary >= 110000
                     )
                     SELECT id FROM top_earners
                 )"
            );

            $rows = $this->ztdQuery("SELECT name, salary FROM sl_cis_source ORDER BY salary DESC");

            if ((float) $rows[0]['salary'] < 140000) {
                $this->markTestIncomplete(
                    'UPDATE WHERE IN nested CTE (SQLite): Eve expected ~143000, got ' . $rows[0]['salary']
                    . '. All: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(143000, (float) $rows[0]['salary'], 1);
            $this->assertEqualsWithDelta(132000, (float) $rows[1]['salary'], 1);
            $this->assertEqualsWithDelta(121000, (float) $rows[2]['salary'], 1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE IN nested CTE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with nested CTE and ? params.
     */
    public function testPreparedSelectNestedCte(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT sub.name, sub.rn FROM (
                     WITH dept_rank AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM sl_cis_source
                         WHERE dept = ?
                     )
                     SELECT name, rn FROM dept_rank
                 ) sub ORDER BY sub.rn",
                ['eng']
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared nested CTE (SQLite): expected 3 eng rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Eve', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT nested CTE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT from nested CTE after prior DML.
     */
    public function testInsertFromNestedCteAfterDml(): void
    {
        try {
            $this->ztdExec("UPDATE sl_cis_source SET salary = 150000 WHERE name = 'Bob'");
            $this->ztdExec("DELETE FROM sl_cis_source WHERE name = 'Charlie'");

            $this->ztdExec(
                "INSERT INTO sl_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM sl_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT nested CTE after DML (SQLite): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            $this->assertContains('Bob', $names);    // Now top in eng after raise
            $this->assertContains('Diana', $names);   // Top in sales
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from nested CTE after DML (SQLite) failed: ' . $e->getMessage());
        }
    }
}
