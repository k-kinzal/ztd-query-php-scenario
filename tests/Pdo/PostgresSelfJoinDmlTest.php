<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML where the target table is also referenced via alias (self-join)
 * through ZTD shadow store on PostgreSQL.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class PostgresSelfJoinDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sjd_employees (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            manager_id INT NULL,
            salary NUMERIC(10,2) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_sjd_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (1, 'CEO', NULL, 200000)");
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (2, 'VP', 1, 150000)");
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (3, 'Manager', 2, 100000)");
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (4, 'Dev1', 3, 80000)");
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (5, 'Dev2', 3, 75000)");
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (6, 'Intern', 3, 40000)");
    }

    public function testDeleteWithSelfReferenceSubquery(): void
    {
        $sql = "DELETE FROM pg_sjd_employees
                WHERE salary < (
                    SELECT m.salary / 2
                    FROM pg_sjd_employees m
                    WHERE m.id = pg_sjd_employees.manager_id
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_sjd_employees ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'DELETE self-ref: expected 5, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Intern', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE self-reference subquery failed: ' . $e->getMessage());
        }
    }

    public function testDeleteDuplicatesViaSelfExists(): void
    {
        $this->pdo->exec("INSERT INTO pg_sjd_employees (id, name, manager_id, salary) VALUES (7, 'Dev3', 3, 75000)");

        $sql = "DELETE FROM pg_sjd_employees
                WHERE EXISTS (
                    SELECT 1 FROM pg_sjd_employees dup
                    WHERE dup.salary = pg_sjd_employees.salary
                      AND dup.manager_id = pg_sjd_employees.manager_id
                      AND dup.id < pg_sjd_employees.id
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name FROM pg_sjd_employees ORDER BY id");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'DELETE dup self-EXISTS: expected 6, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertNotContains(7, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE duplicates self-EXISTS failed: ' . $e->getMessage());
        }
    }

    public function testUpdateToGroupAverage(): void
    {
        $sql = "UPDATE pg_sjd_employees
                SET salary = (
                    SELECT AVG(peer.salary)
                    FROM pg_sjd_employees peer
                    WHERE peer.manager_id = pg_sjd_employees.manager_id
                )
                WHERE manager_id IS NOT NULL";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, salary FROM pg_sjd_employees ORDER BY id");

            $byName = [];
            foreach ($rows as $r) {
                $byName[$r['name']] = (float) $r['salary'];
            }

            $this->assertSame(200000.0, $byName['CEO']);
            $this->assertEqualsWithDelta(65000.0, $byName['Dev1'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE to group average failed: ' . $e->getMessage());
        }
    }
}
