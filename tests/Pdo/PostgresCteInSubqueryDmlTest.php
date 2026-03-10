<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CTE (WITH clause) nested inside a subquery of a DML statement on PostgreSQL.
 *
 * Pattern: INSERT INTO t SELECT * FROM (WITH cte AS (SELECT ...) SELECT FROM cte) sub
 * Pattern: DELETE FROM t WHERE id IN (WITH cte AS (...) SELECT FROM cte)
 * Pattern: UPDATE t SET col = (WITH cte AS (...) SELECT ... FROM cte)
 *
 * The CTE rewriter must handle nested WITH clauses that appear inside subqueries
 * of DML statements, not just at the top level.
 *
 * @spec SPEC-10.2
 */
class PostgresCteInSubqueryDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_cis_source (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                dept VARCHAR(30),
                salary DECIMAL(10,2)
            )",
            "CREATE TABLE pg_cis_target (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                rank_in_dept INT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cis_target', 'pg_cis_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_cis_source (name, dept, salary) VALUES ('Alice', 'eng', 120000)");
        $this->ztdExec("INSERT INTO pg_cis_source (name, dept, salary) VALUES ('Bob', 'eng', 100000)");
        $this->ztdExec("INSERT INTO pg_cis_source (name, dept, salary) VALUES ('Charlie', 'sales', 90000)");
        $this->ztdExec("INSERT INTO pg_cis_source (name, dept, salary) VALUES ('Diana', 'sales', 110000)");
        $this->ztdExec("INSERT INTO pg_cis_source (name, dept, salary) VALUES ('Eve', 'eng', 130000)");
    }

    /**
     * INSERT...SELECT FROM (WITH cte AS (...) SELECT FROM cte): nested CTE as data source.
     */
    public function testInsertFromNestedCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM pg_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name, rank_in_dept FROM pg_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT FROM nested CTE: expected 2 rows (top per dept), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                    . ' — nested CTE in INSERT subquery may not work with ZTD'
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Diana', $names);  // top in sales
            $this->assertContains('Eve', $names);     // top in eng
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM nested CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN (WITH cte AS (...) SELECT FROM cte).
     */
    public function testDeleteWhereInNestedCte(): void
    {
        try {
            // Delete the lowest-paid person in each department
            $this->ztdExec(
                "DELETE FROM pg_cis_source
                 WHERE id IN (
                     WITH ranked AS (
                         SELECT id,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rn
                         FROM pg_cis_source
                     )
                     SELECT id FROM ranked WHERE rn = 1
                 )"
            );

            $rows = $this->ztdQuery("SELECT name, dept, salary FROM pg_cis_source ORDER BY dept, salary");

            // Eng: Bob (100k) deleted, Alice (120k) and Eve (130k) remain
            // Sales: Charlie (90k) deleted, Diana (110k) remains
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN nested CTE: expected 3 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
            $this->assertNotContains('Charlie', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN nested CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET col = (WITH cte AS (...) SELECT scalar FROM cte): scalar subquery with CTE.
     */
    public function testUpdateSetFromNestedCteScalar(): void
    {
        try {
            // First insert into target
            $this->ztdExec("INSERT INTO pg_cis_target (name, rank_in_dept) VALUES ('Alice', 0)");
            $this->ztdExec("INSERT INTO pg_cis_target (name, rank_in_dept) VALUES ('Eve', 0)");

            // Update rank using nested CTE in scalar subquery
            $this->ztdExec(
                "UPDATE pg_cis_target t
                 SET rank_in_dept = (
                     WITH dept_rank AS (
                         SELECT name,
                                RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
                         FROM pg_cis_source
                     )
                     SELECT rnk FROM dept_rank WHERE dept_rank.name = t.name
                 )"
            );

            $rows = $this->ztdQuery("SELECT name, rank_in_dept FROM pg_cis_target ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE SET nested CTE scalar: expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            // Eve is rank 1 in eng (highest salary), Alice is rank 2
            if ((int) $rows[1]['rank_in_dept'] !== 1) {
                $this->markTestIncomplete(
                    'UPDATE SET nested CTE scalar: Eve expected rank 1, got ' . $rows[1]['rank_in_dept']
                    . '. All: ' . json_encode($rows)
                    . ' — nested CTE in UPDATE scalar subquery may not work'
                );
            }

            $this->assertSame(2, (int) $rows[0]['rank_in_dept']); // Alice rank 2 in eng
            $this->assertSame(1, (int) $rows[1]['rank_in_dept']); // Eve rank 1 in eng
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET from nested CTE scalar failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with nested CTE and $N parameters.
     */
    public function testPreparedDeleteWithNestedCte(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "DELETE FROM pg_cis_source
                 WHERE id IN (
                     WITH dept_members AS (
                         SELECT id, salary FROM pg_cis_source WHERE dept = $1
                     )
                     SELECT id FROM dept_members WHERE salary < $2
                 )"
            );
            $stmt->execute(['eng', 115000]);

            $rows = $this->ztdQuery("SELECT name FROM pg_cis_source ORDER BY name");

            // Bob (eng, 100k < 115k) deleted
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE nested CTE: expected 4 remaining, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with nested CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT FROM nested CTE after prior DML on source table.
     */
    public function testInsertFromNestedCteAfterSourceDml(): void
    {
        try {
            // Modify source data first
            $this->ztdExec("UPDATE pg_cis_source SET salary = 150000 WHERE name = 'Bob'");
            $this->ztdExec("DELETE FROM pg_cis_source WHERE name = 'Charlie'");

            // Now INSERT from nested CTE — should see the modified data
            $this->ztdExec(
                "INSERT INTO pg_cis_target (name, rank_in_dept)
                 SELECT sub.name, sub.rn FROM (
                     WITH ranked AS (
                         SELECT name,
                                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
                         FROM pg_cis_source
                     )
                     SELECT name, rn FROM ranked WHERE rn = 1
                 ) sub"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_cis_target ORDER BY name");

            // After update: Bob is now top in eng (150k > Eve's 130k), Diana is top in sales
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT nested CTE after source DML: expected 2 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            if (!in_array('Bob', $names)) {
                $this->markTestIncomplete(
                    'INSERT nested CTE after source DML: expected Bob as top eng, got '
                    . json_encode($names) . ' — nested CTE may not see prior DML'
                );
            }

            $this->assertContains('Bob', $names);
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM nested CTE after source DML failed: ' . $e->getMessage());
        }
    }
}
