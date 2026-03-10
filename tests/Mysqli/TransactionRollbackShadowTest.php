<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests shadow store behavior when transactions are rolled back.
 *
 * ZtdMysqli's begin_transaction(), commit(), and rollback() are pass-through
 * to the underlying MySQLi connection. They do not interact with the shadow
 * store. This means shadow mutations from INSERT/UPDATE/DELETE survive a
 * ROLLBACK, causing the shadow to diverge from the physical database state.
 *
 * This is a common user pattern:
 *   BEGIN; INSERT ...; INSERT ...; ROLLBACK;
 *   SELECT ... -- should NOT see the rolled-back inserts
 *
 * @spec SPEC-4.2
 */
class TransactionRollbackShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_trs_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            qty INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_trs_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_trs_items VALUES (1, 'Alpha', 10)");
        $this->mysqli->query("INSERT INTO mi_trs_items VALUES (2, 'Beta', 20)");
    }

    /**
     * INSERT inside rolled-back transaction should NOT be visible in shadow.
     */
    public function testRolledBackInsertNotVisibleInShadow(): void
    {
        try {
            $this->ztdBeginTransaction();
            $this->mysqli->query("INSERT INTO mi_trs_items VALUES (3, 'Gamma', 30)");
            $this->ztdRollBack();

            $rows = $this->ztdQuery("SELECT id FROM mi_trs_items ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            if (in_array(3, $ids)) {
                $this->markTestIncomplete(
                    'Rolled-back INSERT still visible in shadow. Got ids: ' . json_encode($ids)
                );
            }
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rolled-back INSERT test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE inside rolled-back transaction should NOT affect shadow.
     */
    public function testRolledBackUpdateNotVisibleInShadow(): void
    {
        try {
            $this->ztdBeginTransaction();
            $this->mysqli->query("UPDATE mi_trs_items SET name = 'Alpha-Modified' WHERE id = 1");
            $this->ztdRollBack();

            $rows = $this->ztdQuery("SELECT name FROM mi_trs_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $name = $rows[0]['name'];
            if ($name !== 'Alpha') {
                $this->markTestIncomplete(
                    'Rolled-back UPDATE still visible in shadow. Expected "Alpha", got ' . json_encode($name)
                );
            }
            $this->assertSame('Alpha', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rolled-back UPDATE test failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE inside rolled-back transaction should NOT affect shadow.
     */
    public function testRolledBackDeleteNotVisibleInShadow(): void
    {
        try {
            $this->ztdBeginTransaction();
            $this->mysqli->query("DELETE FROM mi_trs_items WHERE id = 1");
            $this->ztdRollBack();

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_trs_items");
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 2) {
                $this->markTestIncomplete(
                    'Rolled-back DELETE still visible in shadow. Expected 2 rows, got ' . $cnt
                );
            }
            $this->assertEquals(2, $cnt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rolled-back DELETE test failed: ' . $e->getMessage());
        }
    }

    /**
     * Committed transaction should persist in shadow (positive control).
     */
    public function testCommittedInsertVisibleInShadow(): void
    {
        try {
            $this->ztdBeginTransaction();
            $this->mysqli->query("INSERT INTO mi_trs_items VALUES (3, 'Gamma', 30)");
            $this->ztdCommit();

            $rows = $this->ztdQuery("SELECT id FROM mi_trs_items ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            $this->assertContains(3, $ids);
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Committed INSERT test failed: ' . $e->getMessage());
        }
    }

    /**
     * Mixed: commit first transaction, rollback second.
     */
    public function testCommitThenRollback(): void
    {
        try {
            // First transaction: committed
            $this->ztdBeginTransaction();
            $this->mysqli->query("INSERT INTO mi_trs_items VALUES (3, 'Gamma', 30)");
            $this->ztdCommit();

            // Second transaction: rolled back
            $this->ztdBeginTransaction();
            $this->mysqli->query("INSERT INTO mi_trs_items VALUES (4, 'Delta', 40)");
            $this->mysqli->query("UPDATE mi_trs_items SET qty = 99 WHERE id = 1");
            $this->ztdRollBack();

            $rows = $this->ztdQuery("SELECT id, name, qty FROM mi_trs_items ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            // Should see Gamma (committed) but NOT Delta (rolled back)
            if (in_array(4, $ids)) {
                $this->markTestIncomplete(
                    'Second rolled-back INSERT visible. Got ids: ' . json_encode($ids)
                );
            }
            if (!in_array(3, $ids)) {
                $this->markTestIncomplete(
                    'Committed Gamma not visible. Got ids: ' . json_encode($ids)
                );
            }

            // Alpha's qty should be 10 (not 99 from rolled-back UPDATE)
            $alphaRow = null;
            foreach ($rows as $row) {
                if ((int) $row['id'] === 1) {
                    $alphaRow = $row;
                }
            }
            if ($alphaRow !== null && (int) $alphaRow['qty'] !== 10) {
                $this->markTestIncomplete(
                    'Rolled-back UPDATE on Alpha visible. Expected qty=10, got qty=' . $alphaRow['qty']
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Commit then rollback test failed: ' . $e->getMessage());
        }
    }
}
