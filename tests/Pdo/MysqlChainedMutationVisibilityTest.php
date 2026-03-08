<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that interleaved INSERT/UPDATE/DELETE/SELECT operations correctly
 * reflect each mutation step in subsequent reads (MySQL PDO).
 * @spec SPEC-2.2
 */
class MysqlChainedMutationVisibilityTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cmv_items (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                status VARCHAR(20),
                quantity INT,
                price DECIMAL(10,2)
            )',
            'CREATE TABLE mp_cmv_log (
                id INT PRIMARY KEY,
                item_id INT,
                action VARCHAR(20),
                detail VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cmv_log', 'mp_cmv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (1, 'Alpha', 'active', 10, 25.00)");
        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (2, 'Beta', 'active', 20, 15.00)");
        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (3, 'Gamma', 'inactive', 5, 50.00)");
    }

    public function testInsertUpdateDeleteChain(): void
    {
        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cmv_items");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->pdo->exec("UPDATE mp_cmv_items SET quantity = 25, status = 'pending' WHERE id = 4");
        $rows = $this->ztdQuery("SELECT quantity, status FROM mp_cmv_items WHERE id = 4");
        $this->assertSame(25, (int) $rows[0]['quantity']);
        $this->assertSame('pending', $rows[0]['status']);

        $this->pdo->exec("UPDATE mp_cmv_items SET price = price * 1.10 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT price FROM mp_cmv_items WHERE id = 1");
        $this->assertEqualsWithDelta(27.50, (float) $rows[0]['price'], 0.01);

        $this->pdo->exec("DELETE FROM mp_cmv_items WHERE id = 3");
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT SUM(quantity * price) AS total_value FROM mp_cmv_items");
        $this->assertEqualsWithDelta(825.00, (float) $rows[0]['total_value'], 0.01);
    }

    public function testMultipleUpdatesToSameRow(): void
    {
        $this->pdo->exec("UPDATE mp_cmv_items SET quantity = quantity + 5 WHERE id = 1");
        $this->pdo->exec("UPDATE mp_cmv_items SET quantity = quantity + 3 WHERE id = 1");
        $this->pdo->exec("UPDATE mp_cmv_items SET quantity = quantity + 2 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT quantity FROM mp_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']);

        $this->pdo->exec("UPDATE mp_cmv_items SET price = 30.00 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT quantity, price FROM mp_cmv_items WHERE id = 1");
        $this->assertSame(20, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(30.00, (float) $rows[0]['price'], 0.01);
    }

    public function testDeleteAndReInsertSamePk(): void
    {
        $this->pdo->exec("DELETE FROM mp_cmv_items WHERE id = 2");
        $rows = $this->ztdQuery("SELECT id FROM mp_cmv_items WHERE id = 2");
        $this->assertCount(0, $rows);

        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (2, 'Beta-V2', 'active', 100, 99.99)");
        $rows = $this->ztdQuery("SELECT name, quantity, price FROM mp_cmv_items WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame('Beta-V2', $rows[0]['name']);
        $this->assertSame(100, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
    }

    public function testCrossTableMutationVisibility(): void
    {
        $this->pdo->exec("INSERT INTO mp_cmv_log VALUES (1, 1, 'view', 'Viewed Alpha')");
        $this->pdo->exec("INSERT INTO mp_cmv_log VALUES (2, 1, 'update', 'Updated Alpha')");
        $this->pdo->exec("INSERT INTO mp_cmv_log VALUES (3, 2, 'view', 'Viewed Beta')");

        $rows = $this->ztdQuery(
            "SELECT i.name, COUNT(l.id) AS log_count
             FROM mp_cmv_items i
             LEFT JOIN mp_cmv_log l ON l.item_id = i.id
             GROUP BY i.id, i.name
             ORDER BY i.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame(2, (int) $rows[0]['log_count']);
        $this->assertSame(1, (int) $rows[1]['log_count']);
        $this->assertSame(0, (int) $rows[2]['log_count']);

        $this->pdo->exec("UPDATE mp_cmv_items SET status = 'archived' WHERE id = 3");
        $this->pdo->exec("INSERT INTO mp_cmv_log VALUES (4, 3, 'archive', 'Archived Gamma')");

        $rows = $this->ztdQuery(
            "SELECT i.name, i.status, COUNT(l.id) AS log_count
             FROM mp_cmv_items i
             LEFT JOIN mp_cmv_log l ON l.item_id = i.id
             WHERE i.id = 3
             GROUP BY i.id, i.name, i.status"
        );

        $this->assertSame('archived', $rows[0]['status']);
        $this->assertSame(1, (int) $rows[0]['log_count']);
    }

    public function testPhysicalIsolationAfterChain(): void
    {
        $this->pdo->exec("INSERT INTO mp_cmv_items VALUES (4, 'Delta', 'active', 30, 10.00)");
        $this->pdo->exec("UPDATE mp_cmv_items SET quantity = 999 WHERE id = 1");
        $this->pdo->exec("DELETE FROM mp_cmv_items WHERE id = 3");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cmv_items");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_cmv_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
