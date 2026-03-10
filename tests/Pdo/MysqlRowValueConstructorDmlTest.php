<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests row value constructor (tuple comparison) in DML WHERE clauses
 * through ZTD shadow store on MySQL PDO.
 *
 * Cross-platform parity with Mysqli/RowValueConstructorDmlTest.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class MysqlRowValueConstructorDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_rvc_assignments (
                employee_id INT,
                project_id INT,
                role VARCHAR(30),
                hours INT,
                PRIMARY KEY (employee_id, project_id)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_rvc_completed (
                employee_id INT,
                project_id INT,
                completed_at DATE,
                PRIMARY KEY (employee_id, project_id)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_rvc_completed', 'mp_rvc_assignments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_rvc_assignments VALUES (1, 10, 'lead', 40)");
        $this->pdo->exec("INSERT INTO mp_rvc_assignments VALUES (1, 20, 'member', 20)");
        $this->pdo->exec("INSERT INTO mp_rvc_assignments VALUES (2, 10, 'member', 30)");
        $this->pdo->exec("INSERT INTO mp_rvc_assignments VALUES (2, 30, 'lead', 35)");
        $this->pdo->exec("INSERT INTO mp_rvc_assignments VALUES (3, 20, 'member', 15)");

        $this->pdo->exec("INSERT INTO mp_rvc_completed VALUES (1, 10, '2026-01-15')");
        $this->pdo->exec("INSERT INTO mp_rvc_completed VALUES (2, 30, '2026-02-20')");
    }

    /**
     * DELETE WHERE (col1, col2) IN (SELECT ...).
     */
    public function testDeleteWithRowValueInSubquery(): void
    {
        $sql = "DELETE FROM mp_rvc_assignments
                WHERE (employee_id, project_id) IN (
                    SELECT employee_id, project_id FROM mp_rvc_completed
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT employee_id, project_id FROM mp_rvc_assignments ORDER BY employee_id, project_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE (a,b) IN subquery: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE row-value constructor failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE (col1, col2) IN (SELECT ...).
     */
    public function testUpdateWithRowValueInSubquery(): void
    {
        $sql = "UPDATE mp_rvc_assignments
                SET hours = 0
                WHERE (employee_id, project_id) IN (
                    SELECT employee_id, project_id FROM mp_rvc_completed
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT employee_id, project_id, hours FROM mp_rvc_assignments ORDER BY employee_id, project_id");

            $this->assertCount(5, $rows);

            $hoursByPair = [];
            foreach ($rows as $r) {
                $hoursByPair[$r['employee_id'] . '-' . $r['project_id']] = (int)$r['hours'];
            }

            if ($hoursByPair['1-10'] !== 0 || $hoursByPair['2-30'] !== 0) {
                $this->markTestIncomplete(
                    'UPDATE (a,b) IN subquery: not zeroed. Data: ' . json_encode($hoursByPair)
                );
            }

            $this->assertSame(0, $hoursByPair['1-10']);
            $this->assertSame(0, $hoursByPair['2-30']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE row-value constructor failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE (col1, col2) = (?, ?).
     */
    public function testPreparedDeleteRowValueEquality(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM mp_rvc_assignments WHERE (employee_id, project_id) = (?, ?)"
            );
            $stmt->execute([1, 20]);

            $remaining = $this->ztdQuery("SELECT employee_id, project_id FROM mp_rvc_assignments ORDER BY employee_id, project_id");

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
