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
 * Tests that query rewriting occurs at prepare time, not execute time (PostgreSQL PDO).
 */
class PostgresPrepareTimeRewritingTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_ptr_items');
        $raw->exec('CREATE TABLE pg_ptr_items (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2))');
        $raw->exec("INSERT INTO pg_ptr_items VALUES (1, 'Physical A', 10.00)");
        $raw->exec("INSERT INTO pg_ptr_items VALUES (2, 'Physical B', 20.00)");
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testSelectPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->pdo->exec("INSERT INTO pg_ptr_items VALUES (10, 'Shadow X', 99.99)");

        $stmt = $this->pdo->prepare('SELECT * FROM pg_ptr_items WHERE id = ?');

        $this->pdo->disableZtd();

        $stmt->execute([10]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Shadow X', $rows[0]['name']);

        $this->pdo->enableZtd();
    }

    public function testSelectPreparedWithZtdDisabledEnabledBeforeExecute(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->prepare('SELECT * FROM pg_ptr_items ORDER BY id');

        $this->pdo->enableZtd();

        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Physical A', $rows[0]['name']);
        $this->assertSame('Physical B', $rows[1]['name']);
    }

    public function testTwoPreparedStatementsOppositeToggle(): void
    {
        $this->pdo->exec("INSERT INTO pg_ptr_items VALUES (10, 'Shadow Only', 50.00)");

        $stmtShadow = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM pg_ptr_items');

        $this->pdo->disableZtd();
        $stmtPhysical = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM pg_ptr_items');

        $this->pdo->enableZtd();

        $stmtShadow->execute();
        $shadowCount = (int) $stmtShadow->fetch(PDO::FETCH_ASSOC)['cnt'];

        $stmtPhysical->execute();
        $physicalCount = (int) $stmtPhysical->fetch(PDO::FETCH_ASSOC)['cnt'];

        $this->assertSame(1, $shadowCount);
        $this->assertSame(2, $physicalCount);
    }

    public function testReExecuteAcrossMultipleToggles(): void
    {
        $this->pdo->exec("INSERT INTO pg_ptr_items VALUES (1, 'Shadow A', 10.00)");
        $this->pdo->exec("INSERT INTO pg_ptr_items VALUES (2, 'Shadow B', 20.00)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM pg_ptr_items');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdo->disableZtd();
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdo->enableZtd();
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_ptr_items');
    }
}
