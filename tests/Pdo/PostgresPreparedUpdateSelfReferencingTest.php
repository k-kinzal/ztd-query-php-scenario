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
 * Tests prepared UPDATE with self-referencing arithmetic on PostgreSQL PDO.
 *
 * Cross-platform parity with SqlitePreparedUpdateSelfReferencingTest.
 * @spec SPEC-4.2
 */
class PostgresPreparedUpdateSelfReferencingTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_ppupd_test');
        $raw->exec('CREATE TABLE pdo_ppupd_test (id INT PRIMARY KEY, name VARCHAR(50), counter INT, balance DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pdo_ppupd_test VALUES (1, 'Alice', 10, 100.00)");
        $this->pdo->exec("INSERT INTO pdo_ppupd_test VALUES (2, 'Bob', 20, 200.00)");
        $this->pdo->exec("INSERT INTO pdo_ppupd_test VALUES (3, 'Charlie', 30, 300.00)");
    }

    /**
     * Prepared SET col = col + param with parameter.
     */
    public function testPreparedIncrementWithParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pdo_ppupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([5, 1]);

        $qstmt = $this->pdo->query('SELECT counter FROM pdo_ppupd_test WHERE id = 1');
        $this->assertSame(15, (int) $qstmt->fetchColumn());
    }

    /**
     * Prepared decrement.
     */
    public function testPreparedDecrementWithParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pdo_ppupd_test SET balance = balance - ? WHERE id = ?');
        $stmt->execute([25.50, 2]);

        $qstmt = $this->pdo->query('SELECT balance FROM pdo_ppupd_test WHERE id = 2');
        $this->assertEqualsWithDelta(174.50, (float) $qstmt->fetchColumn(), 0.01);
    }

    /**
     * Prepared update all matching rows.
     */
    public function testPreparedUpdateAllMatching(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pdo_ppupd_test SET counter = counter + ? WHERE counter >= ?');
        $stmt->execute([100, 20]);

        $qstmt = $this->pdo->query('SELECT counter FROM pdo_ppupd_test ORDER BY id');
        $rows = $qstmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(10, (int) $rows[0]); // Alice: not >= 20
        $this->assertSame(120, (int) $rows[1]); // Bob: 20 + 100
        $this->assertSame(130, (int) $rows[2]); // Charlie: 30 + 100
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pdo_ppupd_test SET counter = counter + ? WHERE id = ?');
        $stmt->execute([999, 1]);

        $this->pdo->disableZtd();
        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ppupd_test');
        $this->assertSame(0, (int) $qstmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_ppupd_test');
        } catch (\Exception $e) {
        }
    }
}
