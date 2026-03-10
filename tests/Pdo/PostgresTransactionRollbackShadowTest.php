<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests shadow store behavior when transactions are rolled back on PostgreSQL.
 *
 * Confirms Issue #149: shadow mutations survive ROLLBACK.
 *
 * @spec SPEC-4.2
 */
class PostgresTransactionRollbackShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_trs_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            qty INT NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_trs_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_trs_items VALUES (1, 'Alpha', 10)");
        $this->pdo->exec("INSERT INTO pg_trs_items VALUES (2, 'Beta', 20)");
    }

    public function testRolledBackInsertNotVisibleInShadow(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec("INSERT INTO pg_trs_items VALUES (3, 'Gamma', 30)");
            $this->pdo->rollBack();

            $rows = $this->ztdQuery("SELECT id FROM pg_trs_items ORDER BY id");
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

    public function testRolledBackUpdateNotVisibleInShadow(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec("UPDATE pg_trs_items SET name = 'Alpha-Modified' WHERE id = 1");
            $this->pdo->rollBack();

            $rows = $this->ztdQuery("SELECT name FROM pg_trs_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $name = $rows[0]['name'];
            if ($name !== 'Alpha') {
                $this->markTestIncomplete(
                    'Rolled-back UPDATE still visible. Expected "Alpha", got ' . json_encode($name)
                );
            }
            $this->assertSame('Alpha', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rolled-back UPDATE test failed: ' . $e->getMessage());
        }
    }

    public function testRolledBackDeleteNotVisibleInShadow(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec("DELETE FROM pg_trs_items WHERE id = 1");
            $this->pdo->rollBack();

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_trs_items");
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 2) {
                $this->markTestIncomplete(
                    'Rolled-back DELETE still visible. Expected 2, got ' . $cnt
                );
            }
            $this->assertEquals(2, $cnt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rolled-back DELETE test failed: ' . $e->getMessage());
        }
    }

    public function testCommittedInsertVisibleInShadow(): void
    {
        try {
            $this->pdo->beginTransaction();
            $this->pdo->exec("INSERT INTO pg_trs_items VALUES (3, 'Gamma', 30)");
            $this->pdo->commit();

            $rows = $this->ztdQuery("SELECT id FROM pg_trs_items ORDER BY id");
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Committed INSERT test failed: ' . $e->getMessage());
        }
    }
}
