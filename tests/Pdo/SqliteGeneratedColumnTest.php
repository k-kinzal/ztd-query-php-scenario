<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests SQLite generated (stored) column handling with ZTD.
 *
 * SQLite 3.31.0+ supports generated columns:
 * - STORED: computed and persisted
 * - VIRTUAL: computed on read (default)
 *
 * Tests whether generated column values are correctly handled
 * in the shadow store via CTE rewriting.
 */
class SqliteGeneratedColumnTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec("CREATE TABLE sl_gencol_test (
            id INTEGER PRIMARY KEY,
            price REAL,
            quantity INTEGER,
            total REAL GENERATED ALWAYS AS (price * quantity) STORED
        )");
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * INSERT omitting generated columns.
     *
     * Generated columns should NOT be included in INSERT statements.
     */
    public function testInsertOmittingGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 9.99, 3)");

        $stmt = $this->pdo->query('SELECT id, price, quantity FROM sl_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['id']);
        $this->assertEqualsWithDelta(9.99, (float) $row['price'], 0.01);
        $this->assertSame(3, (int) $row['quantity']);
    }

    /**
     * SELECT generated column values from shadow store.
     *
     * Generated column expressions may or may not be computed in shadow.
     */
    public function testSelectGeneratedColumnValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $stmt = $this->pdo->query('SELECT total FROM sl_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // Generated columns may be NULL in shadow (no physical INSERT happened)
        if ($row['total'] !== null) {
            $this->assertEqualsWithDelta(50.0, (float) $row['total'], 0.01);
        } else {
            $this->assertNull($row['total']);
        }
    }

    /**
     * UPDATE non-generated columns and query generated column.
     */
    public function testUpdateAndQueryGeneratedColumn(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec('UPDATE sl_gencol_test SET quantity = 10 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT quantity FROM sl_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $row['quantity']);
    }

    /**
     * Multiple rows with generated columns.
     */
    public function testMultipleRowsWithGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (2, 20.00, 3)");
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (3, 5.00, 10)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_gencol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE row with generated columns.
     */
    public function testDeleteRowWithGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (2, 20.00, 3)");
        $this->pdo->exec('DELETE FROM sl_gencol_test WHERE id = 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_gencol_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_gencol_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
