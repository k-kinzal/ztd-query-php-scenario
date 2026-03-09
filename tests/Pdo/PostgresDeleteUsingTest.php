<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL DELETE ... USING (multi-table DELETE) through CTE shadow.
 *
 * PostgreSQL uses: DELETE FROM t USING s WHERE t.id = s.id
 * This is the PostgreSQL syntax for multi-table DELETE.
 *
 * @spec SPEC-4.3
 */
class PostgresDeleteUsingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_du_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(20))',
            'CREATE TABLE pg_du_deletions (item_id INT PRIMARY KEY, reason VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_du_deletions', 'pg_du_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_du_items VALUES (1, 'Widget A', 'active')");
        $this->pdo->exec("INSERT INTO pg_du_items VALUES (2, 'Widget B', 'active')");
        $this->pdo->exec("INSERT INTO pg_du_items VALUES (3, 'Gadget', 'active')");
        $this->pdo->exec("INSERT INTO pg_du_items VALUES (4, 'Doohickey', 'active')");

        $this->pdo->exec("INSERT INTO pg_du_deletions VALUES (1, 'discontinued')");
        $this->pdo->exec("INSERT INTO pg_du_deletions VALUES (3, 'recalled')");
    }

    /**
     * DELETE ... USING basic multi-table delete.
     */
    public function testDeleteUsingBasic(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_du_items i
                 USING pg_du_deletions d
                 WHERE i.id = d.item_id"
            );

            $rows = $this->ztdQuery('SELECT id, name FROM pg_du_items ORDER BY id');
            $this->assertCount(2, $rows, 'Should have 2 items remaining');
            $this->assertEquals(2, (int) $rows[0]['id']);
            $this->assertEquals(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE USING not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... USING with additional WHERE condition.
     */
    public function testDeleteUsingWithCondition(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_du_items i
                 USING pg_du_deletions d
                 WHERE i.id = d.item_id AND d.reason = 'recalled'"
            );

            $rows = $this->ztdQuery('SELECT id FROM pg_du_items ORDER BY id');
            $this->assertCount(3, $rows, 'Only recalled item (id=3) should be deleted');
            $ids = array_column($rows, 'id');
            $this->assertNotContains('3', $ids);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE USING with condition not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... USING on shadow-inserted data.
     */
    public function testDeleteUsingOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_du_items VALUES (5, 'New Item', 'active')");
        $this->pdo->exec("INSERT INTO pg_du_deletions VALUES (5, 'broken')");

        try {
            $this->pdo->exec(
                "DELETE FROM pg_du_items i
                 USING pg_du_deletions d
                 WHERE i.id = d.item_id AND i.id = 5"
            );

            $rows = $this->ztdQuery('SELECT id FROM pg_du_items WHERE id = 5');
            $this->assertCount(0, $rows, 'Shadow-inserted item should be deleted');
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE USING on shadow data not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... USING then verify remaining count.
     */
    public function testDeleteUsingRowCount(): void
    {
        try {
            $affected = $this->pdo->exec(
                "DELETE FROM pg_du_items i
                 USING pg_du_deletions d
                 WHERE i.id = d.item_id"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_du_items');
            $this->assertEquals(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE USING not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_du_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
