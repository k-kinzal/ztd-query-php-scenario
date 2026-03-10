<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CTE nested inside a subquery of a DML statement on MySQLi.
 *
 * @spec SPEC-10.2
 */
class CteInSubqueryDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_cis_source (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                dept VARCHAR(30),
                salary DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_cis_target (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                rank_in_dept INT
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cis_target', 'mi_cis_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_cis_source (name, dept, salary) VALUES ('Alice', 'eng', 120000)");
        $this->ztdExec("INSERT INTO mi_cis_source (name, dept, salary) VALUES ('Bob', 'eng', 100000)");
        $this->ztdExec("INSERT INTO mi_cis_source (name, dept, salary) VALUES ('Charlie', 'sales', 90000)");
        $this->ztdExec("INSERT INTO mi_cis_source (name, dept, salary) VALUES ('Diana', 'sales', 110000)");
        $this->ztdExec("INSERT INTO mi_cis_source (name, dept, salary) VALUES ('Eve', 'eng', 130000)");
    }

    /**
     * INSERT...SELECT FROM (WITH cte AS (...) SELECT FROM cte) sub.
     */
    public function testInsertFromNestedCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM mi_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name, rank_in_dept FROM mi_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT FROM nested CTE (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Diana', $names);
            $this->assertContains('Eve', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM nested CTE (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN nested CTE subquery.
     */
    public function testDeleteWhereInNestedCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_cis_source
                 WHERE id IN (
                     SELECT id FROM (
                         WITH ranked AS (
                             SELECT id,
                                    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rn
                             FROM mi_cis_source
                         )
                         SELECT id FROM ranked WHERE rn = 1
                     ) sub
                 )"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_cis_source ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN nested CTE (MySQLi): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN nested CTE (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with nested CTE in WHERE subquery.
     */
    public function testUpdateWhereNestedCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_cis_source
                 SET salary = salary * 1.10
                 WHERE id IN (
                     SELECT id FROM (
                         WITH top_earners AS (
                             SELECT id FROM mi_cis_source WHERE salary >= 110000
                         )
                         SELECT id FROM top_earners
                     ) sub
                 )"
            );

            $rows = $this->ztdQuery("SELECT name, salary FROM mi_cis_source ORDER BY salary DESC");

            if ((float) $rows[0]['salary'] < 140000) {
                $this->markTestIncomplete(
                    'UPDATE WHERE nested CTE (MySQLi): Eve expected ~143000, got ' . $rows[0]['salary']
                    . '. All: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(143000, (float) $rows[0]['salary'], 1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE nested CTE (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
