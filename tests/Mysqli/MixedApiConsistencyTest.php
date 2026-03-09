<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests shadow store consistency when mixing query() and prepare() on MySQLi.
 *
 * @spec SPEC-2.1
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class MixedApiConsistencyTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mac_items (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\',
            qty INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mac_items'];
    }

    public function testQueryInsertVisibleToQuery(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    public function testQueryInsertVisibleToPrepare(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Alpha', 10)");
        $rows = $this->ztdPrepareAndExecute("SELECT name FROM mac_items WHERE id = ?", [1]);
        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
    }

    public function testInsertUpdateQuerySequence(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Gamma', 5)");
        $this->ztdExec("UPDATE mac_items SET qty = 15 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT qty FROM mac_items WHERE id = 1");
        $this->assertSame(15, (int) $rows[0]['qty']);
    }

    public function testDeleteAndReinsertSamePk(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Original', 10)");
        $this->ztdExec("DELETE FROM mac_items WHERE id = 1");
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Replaced', 99)");

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Replaced', $rows[0]['name']);
    }

    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->ztdExec("INSERT INTO mac_items (id, name, qty) VALUES (1, 'Start', 0)");
        $this->ztdExec("UPDATE mac_items SET qty = 10 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET qty = 20 WHERE id = 1");
        $this->ztdExec("UPDATE mac_items SET name = 'End' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, qty FROM mac_items WHERE id = 1");
        $this->assertSame('End', $rows[0]['name']);
        $this->assertSame(20, (int) $rows[0]['qty']);
    }

    public function testComplexMutationThenAggregate(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->ztdExec("INSERT INTO mac_items (id, name, status, qty) VALUES ($i, 'Item$i', 'active', " . ($i * 10) . ")");
        }
        $this->ztdExec("UPDATE mac_items SET status = 'inactive' WHERE id IN (2, 4)");
        $this->ztdExec("DELETE FROM mac_items WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT status, SUM(qty) AS total, COUNT(*) AS cnt
             FROM mac_items
             GROUP BY status
             ORDER BY status"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['status']);
        $this->assertSame(60, (int) $rows[0]['total']);
    }
}
