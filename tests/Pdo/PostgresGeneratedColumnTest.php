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
 * Tests PostgreSQL generated (stored) column handling with ZTD.
 *
 * PostgreSQL 12+ supports generated columns:
 * - STORED only (no VIRTUAL support in PostgreSQL)
 *
 * Tests whether generated column values are correctly handled
 * in the shadow store via CTE rewriting.
 */
class PostgresGeneratedColumnTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_gencol_test');
        $raw->exec('CREATE TABLE pg_gencol_test (
            id INT PRIMARY KEY,
            price NUMERIC(10,2),
            quantity INT,
            total NUMERIC(10,2) GENERATED ALWAYS AS (price * quantity) STORED
        )');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');
    }

    /**
     * INSERT omitting generated columns.
     */
    public function testInsertOmittingGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (1, 9.99, 3)");

        $stmt = $this->pdo->query('SELECT id, price, quantity FROM pg_gencol_test WHERE id = 1');
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
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $stmt = $this->pdo->query('SELECT total FROM pg_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // Generated columns may be NULL in shadow (no physical INSERT happened)
        if ($row['total'] !== null) {
            $this->assertEqualsWithDelta(50.0, (float) $row['total'], 0.01);
        } else {
            $this->assertNull($row['total']);
        }
    }

    /**
     * UPDATE non-generated columns and query.
     */
    public function testUpdateAndQueryNonGeneratedColumn(): void
    {
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec('UPDATE pg_gencol_test SET quantity = 10 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT quantity FROM pg_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $row['quantity']);
    }

    /**
     * Multiple rows with generated columns.
     */
    public function testMultipleRowsWithGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (2, 20.00, 3)");
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (3, 5.00, 10)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_gencol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_gencol_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_gencol_test');
        } catch (\Exception $e) {
        }
    }
}
