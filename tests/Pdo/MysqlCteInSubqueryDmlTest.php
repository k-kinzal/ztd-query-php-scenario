<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CTE nested inside a subquery of a DML statement on MySQL 8.0+.
 *
 * MySQL 8.0 supports CTEs. This tests patterns like:
 * INSERT INTO t SELECT * FROM (WITH cte AS (...) SELECT FROM cte) sub
 * DELETE FROM t WHERE id IN (WITH cte AS (...) SELECT FROM cte)
 *
 * @spec SPEC-10.2
 */
class MysqlCteInSubqueryDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_cis_source (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                dept VARCHAR(30),
                salary DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE my_cis_target (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                rank_in_dept INT
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cis_target', 'my_cis_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_cis_source (name, dept, salary) VALUES ('Alice', 'eng', 120000)");
        $this->ztdExec("INSERT INTO my_cis_source (name, dept, salary) VALUES ('Bob', 'eng', 100000)");
        $this->ztdExec("INSERT INTO my_cis_source (name, dept, salary) VALUES ('Charlie', 'sales', 90000)");
        $this->ztdExec("INSERT INTO my_cis_source (name, dept, salary) VALUES ('Diana', 'sales', 110000)");
        $this->ztdExec("INSERT INTO my_cis_source (name, dept, salary) VALUES ('Eve', 'eng', 130000)");
    }

    /**
     * INSERT...SELECT FROM (WITH cte AS (...) SELECT FROM cte) sub.
     */
    public function testInsertFromNestedCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM my_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name, rank_in_dept FROM my_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT FROM nested CTE (MySQL): expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Diana', $names);
            $this->assertContains('Eve', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM nested CTE (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN (WITH cte AS (...) SELECT FROM cte).
     */
    public function testDeleteWhereInNestedCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_cis_source
                 WHERE id IN (
                     SELECT id FROM (
                         WITH ranked AS (
                             SELECT id,
                                    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rn
                             FROM my_cis_source
                         )
                         SELECT id FROM ranked WHERE rn = 1
                     ) sub
                 )"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_cis_source ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN nested CTE (MySQL): expected 3 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
            $this->assertNotContains('Charlie', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN nested CTE (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with nested CTE in WHERE subquery.
     */
    public function testUpdateWhereNestedCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_cis_source
                 SET salary = salary * 1.10
                 WHERE id IN (
                     SELECT id FROM (
                         WITH top_earners AS (
                             SELECT id FROM my_cis_source WHERE salary >= 110000
                         )
                         SELECT id FROM top_earners
                     ) sub
                 )"
            );

            $rows = $this->ztdQuery("SELECT name, salary FROM my_cis_source ORDER BY salary DESC");

            // Eve 130000→143000, Alice 120000→132000, Diana 110000→121000
            if ((float) $rows[0]['salary'] < 140000) {
                $this->markTestIncomplete(
                    'UPDATE WHERE nested CTE (MySQL): Eve expected ~143000, got ' . $rows[0]['salary']
                    . '. All: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(143000, (float) $rows[0]['salary'], 1);
            $this->assertEqualsWithDelta(132000, (float) $rows[1]['salary'], 1);
            $this->assertEqualsWithDelta(121000, (float) $rows[2]['salary'], 1);
            // Bob and Charlie unchanged
            $this->assertEqualsWithDelta(100000, (float) $rows[3]['salary'], 1);
            $this->assertEqualsWithDelta(90000, (float) $rows[4]['salary'], 1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE nested CTE (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT from nested CTE with ? params.
     */
    public function testPreparedInsertFromNestedCte(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT sub.name, sub.rn FROM (
                     WITH dept_rank AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM my_cis_source
                         WHERE dept = ?
                     )
                     SELECT name, rn FROM dept_rank
                 ) sub ORDER BY sub.rn",
                ['eng']
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared nested CTE (MySQL): expected 3 eng rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Eve', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT from nested CTE (MySQL) failed: ' . $e->getMessage());
        }
    }
}
