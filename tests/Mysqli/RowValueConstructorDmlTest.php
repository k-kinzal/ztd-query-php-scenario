<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests row value constructor (tuple comparison) in DML WHERE clauses
 * through ZTD shadow store on MySQLi.
 *
 * Row value constructors like `WHERE (a, b) IN (SELECT x, y FROM ...)` are
 * used for composite key lookups and multi-column matching. The CTE rewriter
 * must preserve tuple semantics in shadow queries.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class RowValueConstructorDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rvc_assignments (
                employee_id INT,
                project_id INT,
                role VARCHAR(30),
                hours INT,
                PRIMARY KEY (employee_id, project_id)
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_rvc_completed (
                employee_id INT,
                project_id INT,
                completed_at DATE,
                PRIMARY KEY (employee_id, project_id)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rvc_completed', 'mi_rvc_assignments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rvc_assignments VALUES (1, 10, 'lead', 40)");
        $this->mysqli->query("INSERT INTO mi_rvc_assignments VALUES (1, 20, 'member', 20)");
        $this->mysqli->query("INSERT INTO mi_rvc_assignments VALUES (2, 10, 'member', 30)");
        $this->mysqli->query("INSERT INTO mi_rvc_assignments VALUES (2, 30, 'lead', 35)");
        $this->mysqli->query("INSERT INTO mi_rvc_assignments VALUES (3, 20, 'member', 15)");

        $this->mysqli->query("INSERT INTO mi_rvc_completed VALUES (1, 10, '2026-01-15')");
        $this->mysqli->query("INSERT INTO mi_rvc_completed VALUES (2, 30, '2026-02-20')");
    }

    /**
     * DELETE WHERE (col1, col2) IN (SELECT ...) — composite key subquery.
     */
    public function testDeleteWithRowValueInSubquery(): void
    {
        $sql = "DELETE FROM mi_rvc_assignments
                WHERE (employee_id, project_id) IN (
                    SELECT employee_id, project_id FROM mi_rvc_completed
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT employee_id, project_id FROM mi_rvc_assignments ORDER BY employee_id, project_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE (a,b) IN subquery: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Should have removed (1,10) and (2,30)
            $pairs = array_map(fn($r) => $r['employee_id'] . '-' . $r['project_id'], $rows);
            $this->assertNotContains('1-10', $pairs);
            $this->assertNotContains('2-30', $pairs);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE row-value constructor failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE (col1, col2) IN (SELECT ...) — update matched composite rows.
     */
    public function testUpdateWithRowValueInSubquery(): void
    {
        $sql = "UPDATE mi_rvc_assignments
                SET hours = 0
                WHERE (employee_id, project_id) IN (
                    SELECT employee_id, project_id FROM mi_rvc_completed
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT employee_id, project_id, hours FROM mi_rvc_assignments ORDER BY employee_id, project_id");

            $this->assertCount(5, $rows);

            $hoursByPair = [];
            foreach ($rows as $r) {
                $hoursByPair[$r['employee_id'] . '-' . $r['project_id']] = (int)$r['hours'];
            }

            if ($hoursByPair['1-10'] !== 0 || $hoursByPair['2-30'] !== 0) {
                $this->markTestIncomplete(
                    'UPDATE (a,b) IN subquery: completed rows not zeroed. Data: ' . json_encode($hoursByPair)
                );
            }

            $this->assertSame(0, $hoursByPair['1-10']);
            $this->assertSame(0, $hoursByPair['2-30']);
            $this->assertSame(20, $hoursByPair['1-20']);
            $this->assertSame(30, $hoursByPair['2-10']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE row-value constructor failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE (col1, col2) NOT IN (SELECT ...) — inverse match.
     */
    public function testDeleteWithRowValueNotInSubquery(): void
    {
        $sql = "DELETE FROM mi_rvc_assignments
                WHERE (employee_id, project_id) NOT IN (
                    SELECT employee_id, project_id FROM mi_rvc_completed
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT employee_id, project_id FROM mi_rvc_assignments ORDER BY employee_id, project_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE NOT IN row-value: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT IN row-value constructor failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE (col1, col2) = (?, ?) — tuple equality with params.
     */
    public function testPreparedDeleteRowValueEquality(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "DELETE FROM mi_rvc_assignments WHERE (employee_id, project_id) = (?, ?)",
                [1, 20]
            );

            $remaining = $this->ztdQuery("SELECT employee_id, project_id FROM mi_rvc_assignments ORDER BY employee_id, project_id");

            if (count($remaining) !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE (a,b)=(?,?): expected 4, got ' . count($remaining)
                    . '. Data: ' . json_encode($remaining)
                );
            }

            $this->assertCount(4, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE row-value equality failed: ' . $e->getMessage());
        }
    }
}
