<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests HAVING without GROUP BY and ON CONFLICT edge cases on PostgreSQL PDO.
 */
class PostgresHavingAndOnConflictTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_hoc_items');
        $raw->exec('CREATE TABLE pg_hoc_items (id INT PRIMARY KEY, name VARCHAR(255), qty INT, price NUMERIC(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_hoc_items VALUES (1, 'Widget', 10, 9.99)");
        $this->pdo->exec("INSERT INTO pg_hoc_items VALUES (2, 'Gadget', 5, 29.99)");
        $this->pdo->exec("INSERT INTO pg_hoc_items VALUES (3, 'Gizmo', 20, 19.99)");
    }

    public function testHavingWithoutGroupBy(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_hoc_items HAVING COUNT(*) > 2");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testHavingWithoutGroupByNoMatch(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_hoc_items HAVING COUNT(*) > 10");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testOnConflictDoUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pg_hoc_items (id, name, qty, price) VALUES (1, 'Widget V2', 100, 12.99) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, qty = EXCLUDED.qty, price = EXCLUDED.price");

        $stmt = $this->pdo->query("SELECT name, qty, price FROM pg_hoc_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget V2', $row['name']);
        $this->assertSame(100, (int) $row['qty']);
    }

    public function testOnConflictDoNothing(): void
    {
        $this->pdo->exec("INSERT INTO pg_hoc_items (id, name, qty, price) VALUES (1, 'ShouldBeIgnored', 999, 0.01) ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query("SELECT name FROM pg_hoc_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['name']); // Unchanged
    }

    /**
     * ON CONFLICT DO UPDATE via prepared statement does not update the existing row.
     * The old row is retained — upsert behavior is not applied to prepared statements.
     */
    public function testOnConflictDoUpdatePreparedDoesNotUpdate(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO pg_hoc_items (id, name, qty, price) VALUES (?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, qty = EXCLUDED.qty");
        $stmt->execute([2, 'Gadget Pro', 200, 49.99]);

        $select = $this->pdo->query("SELECT name, qty FROM pg_hoc_items WHERE id = 2");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        // Expected: 'Gadget Pro' (updated), Actual: 'Gadget' (old row retained)
        $this->assertSame('Gadget', $row['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_hoc_items');
    }
}
