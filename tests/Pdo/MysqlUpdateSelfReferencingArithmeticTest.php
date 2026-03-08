<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests UPDATE with self-referencing arithmetic (SET col = col + N)
 * on MySQL, verifying cross-platform parity with SQLite.
 */
class MysqlUpdateSelfReferencingArithmeticTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_selfref_test');
        $raw->exec('CREATE TABLE pdo_selfref_test (id INT PRIMARY KEY, name VARCHAR(50), counter INT, balance DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_selfref_test VALUES (1, 'Alice', 10, 100.00)");
        $this->pdo->exec("INSERT INTO pdo_selfref_test VALUES (2, 'Bob', 20, 200.00)");
        $this->pdo->exec("INSERT INTO pdo_selfref_test VALUES (3, 'Charlie', 30, 300.00)");
    }

    /**
     * SET col = col + N on a single row.
     */
    public function testIncrementSingleRow(): void
    {
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 5 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT counter FROM pdo_selfref_test WHERE id = 1');
        $this->assertSame(15, (int) $stmt->fetchColumn());
    }

    /**
     * Multiple sequential self-referencing updates.
     */
    public function testSequentialSelfReferencingUpdates(): void
    {
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 1 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT counter FROM pdo_selfref_test WHERE id = 1');
        $this->assertSame(13, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing update on all rows.
     */
    public function testSelfReferencingUpdateAllRows(): void
    {
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 100');

        $stmt = $this->pdo->query('SELECT counter FROM pdo_selfref_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(110, (int) $rows[0]);
        $this->assertSame(120, (int) $rows[1]);
        $this->assertSame(130, (int) $rows[2]);
    }

    /**
     * Self-referencing update after INSERT.
     */
    public function testSelfReferencingUpdateAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_selfref_test VALUES (4, 'Diana', 0, 0.00)");
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 50 WHERE id = 4');

        $stmt = $this->pdo->query('SELECT counter FROM pdo_selfref_test WHERE id = 4');
        $this->assertSame(50, (int) $stmt->fetchColumn());
    }

    /**
     * Cross-column self-referencing.
     */
    public function testCrossColumnSelfReferencing(): void
    {
        $this->pdo->exec('UPDATE pdo_selfref_test SET balance = balance + counter WHERE id = 1');

        $stmt = $this->pdo->query('SELECT balance FROM pdo_selfref_test WHERE id = 1');
        $this->assertEqualsWithDelta(110.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('UPDATE pdo_selfref_test SET counter = counter + 999');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_selfref_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_selfref_test');
        } catch (\Exception $e) {
        }
    }
}
