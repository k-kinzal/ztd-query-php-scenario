<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests that interleaved INSERT/UPDATE/DELETE/SELECT operations correctly
 * reflect each mutation step in subsequent reads (PostgreSQL PDO).
 * @spec SPEC-2.2
 */
class PostgresChainedMutationVisibilityTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cmv_items (
                id INTEGER PRIMARY KEY,
                name TEXT,
                status TEXT,
                quantity INTEGER,
                price NUMERIC(10,2)
            )',
            'CREATE TABLE pg_cmv_log (
                id INTEGER PRIMARY KEY,
                item_id INTEGER,
                action TEXT,
                detail TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cmv_log', 'pg_cmv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (1, 'Alpha', 'active', 10, 25.00)");
        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (2, 'Beta', 'active', 20, 15.00)");
        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (3, 'Gamma', 'inactive', 5, 50.00)");
    }

    public function testInsertUpdateDeleteChain(): void
    {
        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cmv_items");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->exec("UPDATE pg_cmv_items SET quantity = 25, status = 'pending' WHERE id = 4");
        $rows = $this->ztdQuery("SELECT quantity, status FROM pg_cmv_items WHERE id = 4");
        $this->assertSame(25, (int) $rows[0]['quantity']);
        $this->assertSame('pending', $rows[0]['status']);

        $this->pdo->exec("UPDATE pg_cmv_items SET price = price * 1.10 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT price FROM pg_cmv_items WHERE id = 1");
        $this->assertEqualsWithDelta(27.50, (float) $rows[0]['price'], 0.01);

        $this->pdo->exec("DELETE FROM pg_cmv_items WHERE id = 3");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT SUM(quantity * price) AS total_value FROM pg_cmv_items");
        $this->assertEqualsWithDelta(825.00, (float) $rows[0]['total_value'], 0.01);
    }

    public function testMultipleUpdatesToSameRow(): void
    {
        $this->pdo->exec("UPDATE pg_cmv_items SET quantity = quantity + 5 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_cmv_items SET quantity = quantity + 3 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_cmv_items SET quantity = quantity + 2 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT quantity FROM pg_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']);

        $this->pdo->exec("UPDATE pg_cmv_items SET price = 30.00 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT quantity, price FROM pg_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(30.00, (float) $rows[0]['price'], 0.01);
    }

    public function testDeleteAndReInsertSamePk(): void
    {
        $this->pdo->exec("DELETE FROM pg_cmv_items WHERE id = 2");
        $rows = $this->ztdQuery("SELECT id FROM pg_cmv_items WHERE id = 2");
        $this->assertCount(0, $rows);

        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (2, 'Beta-V2', 'active', 100, 99.99)");
        $rows = $this->ztdQuery("SELECT name, quantity, price FROM pg_cmv_items WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame('Beta-V2', $rows[0]['name']);
        $this->assertSame(100, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
    }

    public function testCrossTableMutationVisibility(): void
    {
        $this->pdo->exec("INSERT INTO pg_cmv_log VALUES (1, 1, 'view', 'Viewed Alpha')");
        $this->pdo->exec("INSERT INTO pg_cmv_log VALUES (2, 1, 'update', 'Updated Alpha')");
        $this->pdo->exec("INSERT INTO pg_cmv_log VALUES (3, 2, 'view', 'Viewed Beta')");

        $rows = $this->ztdQuery(
            "SELECT i.name, COUNT(l.id) AS log_count
             FROM pg_cmv_items i
             LEFT JOIN pg_cmv_log l ON l.item_id = i.id
             GROUP BY i.id, i.name
             ORDER BY i.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame(2, (int) $rows[0]['log_count']);
        $this->assertSame(1, (int) $rows[1]['log_count']);
        $this->assertSame(0, (int) $rows[2]['log_count']);

        $this->pdo->exec("UPDATE pg_cmv_items SET status = 'archived' WHERE id = 3");
        $this->pdo->exec("INSERT INTO pg_cmv_log VALUES (4, 3, 'archive', 'Archived Gamma')");

        $rows = $this->ztdQuery(
            "SELECT i.name, i.status, COUNT(l.id) AS log_count
             FROM pg_cmv_items i
             LEFT JOIN pg_cmv_log l ON l.item_id = i.id
             WHERE i.id = 3
             GROUP BY i.id, i.name, i.status"
        );

        $this->assertSame('archived', $rows[0]['status']);
        $this->assertSame(1, (int) $rows[0]['log_count']);
    }

    public function testPhysicalIsolationAfterChain(): void
    {
        $this->pdo->exec("INSERT INTO pg_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $this->pdo->exec("UPDATE pg_cmv_items SET quantity = 999 WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_cmv_items WHERE id = 3");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_cmv_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
