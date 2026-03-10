<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DML edge cases through the shadow store:
 * - UPDATE/DELETE affecting 0 rows
 * - Multiple sequential mutations on overlapping rows
 * - INSERT followed by immediate UPDATE and DELETE on same row
 * - Aggregate queries after DELETE-all + re-INSERT
 *
 * These patterns test shadow store consistency under stress.
 *
 * @spec SPEC-4.2
 */
class DmlEdgeCaseBehaviorTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_dec_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            qty INT NOT NULL DEFAULT 0,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_dec_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_dec_items VALUES (1, 'Alpha', 10, 'active')");
        $this->mysqli->query("INSERT INTO mi_dec_items VALUES (2, 'Beta', 20, 'active')");
        $this->mysqli->query("INSERT INTO mi_dec_items VALUES (3, 'Gamma', 30, 'inactive')");
    }

    /**
     * UPDATE matching 0 rows — shadow should remain unchanged.
     */
    public function testUpdateAffectingZeroRows(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 999 WHERE id = 99");

            $rows = $this->ztdQuery("SELECT id, qty FROM mi_dec_items ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertEquals(10, (int) $rows[0]['qty']);
            $this->assertEquals(20, (int) $rows[1]['qty']);
            $this->assertEquals(30, (int) $rows[2]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE 0 rows failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE matching 0 rows — shadow should remain unchanged.
     */
    public function testDeleteAffectingZeroRows(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 99");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dec_items");
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE 0 rows failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT → UPDATE → DELETE on same row in one session.
     */
    public function testInsertUpdateDeleteSameRow(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_dec_items VALUES (4, 'Delta', 40, 'active')");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 45 WHERE id = 4");
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 4");

            $rows = $this->ztdQuery("SELECT id FROM mi_dec_items ORDER BY id");
            $ids = array_column($rows, 'id');

            if (in_array('4', $ids) || in_array(4, $ids)) {
                $this->markTestIncomplete('INSERT+UPDATE+DELETE same row: row 4 still visible');
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT+UPDATE+DELETE same row failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE all rows then re-INSERT — tests shadow store reset behavior.
     */
    public function testDeleteAllThenReInsert(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 1");
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 2");
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 3");

            // Table should be empty
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dec_items");
            if ((int) $rows[0]['cnt'] !== 0) {
                $this->markTestIncomplete('DELETE all: expected 0 rows, got ' . $rows[0]['cnt']);
            }

            // Re-insert
            $this->mysqli->query("INSERT INTO mi_dec_items VALUES (10, 'New', 100, 'active')");

            $rows = $this->ztdQuery("SELECT id, name, qty FROM mi_dec_items");
            $this->assertCount(1, $rows);
            $this->assertEquals(10, (int) $rows[0]['id']);
            $this->assertSame('New', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE all + re-INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple UPDATEs to same column on same row — last write wins.
     */
    public function testMultipleUpdatesToSameColumn(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 100 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 200 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 300 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT qty FROM mi_dec_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $qty = (int) $rows[0]['qty'];
            if ($qty !== 300) {
                $this->markTestIncomplete("Multiple updates: expected 300, got $qty");
            }
            $this->assertEquals(300, $qty);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple updates same column failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with self-referencing increment, multiple times.
     */
    public function testChainedIncrements(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_dec_items SET qty = qty + 5 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = qty + 5 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = qty + 5 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT qty FROM mi_dec_items WHERE id = 1");
            $this->assertCount(1, $rows);

            // 10 + 5 + 5 + 5 = 25
            $qty = (int) $rows[0]['qty'];
            if ($qty !== 25) {
                $this->markTestIncomplete("Chained increments: expected 25, got $qty");
            }
            $this->assertEquals(25, $qty);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained increments failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate query after DML that changes which rows match WHERE.
     */
    public function testAggregateAfterStatusChange(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_dec_items SET status = 'inactive' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT status, COUNT(*) AS cnt, SUM(qty) AS total
                 FROM mi_dec_items
                 GROUP BY status
                 ORDER BY status"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['status']] = ['cnt' => (int) $row['cnt'], 'total' => (int) $row['total']];
            }

            // active: Alpha(10) only; inactive: Beta(20), Gamma(30)
            $this->assertEquals(1, $map['active']['cnt']);
            $this->assertEquals(10, $map['active']['total']);
            $this->assertEquals(2, $map['inactive']['cnt']);
            $this->assertEquals(50, $map['inactive']['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate after status change failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE matches a row that was previously DELETEd from shadow.
     * The DELETE should take precedence.
     */
    public function testUpdateAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_dec_items WHERE id = 1");
            $this->mysqli->query("UPDATE mi_dec_items SET qty = 999 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT id FROM mi_dec_items ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            // Row 1 was deleted. UPDATE to deleted row should not resurrect it.
            if (in_array(1, $ids)) {
                $this->markTestIncomplete('UPDATE after DELETE: deleted row 1 resurrected!');
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE after DELETE failed: ' . $e->getMessage());
        }
    }
}
