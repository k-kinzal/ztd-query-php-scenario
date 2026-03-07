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
 * Tests savepoint behavior with ZTD on PostgreSQL PDO.
 *
 * PostgreSQL rewriter silently passes SAVEPOINT/RELEASE/ROLLBACK TO through,
 * but the shadow store does not participate in savepoints — shadow data
 * persists regardless of savepoint rollback.
 */
class PostgresSavepointTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS sp_test');
        $raw->exec('CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO sp_test VALUES (1, 'Alice')");
    }

    public function testSavepointPassesThroughSilently(): void
    {
        $this->pdo->beginTransaction();
        $result = $this->pdo->exec('SAVEPOINT sp1');
        // Does not throw — silently passes through
        $this->assertSame(0, $result);
        $this->pdo->commit();
    }

    public function testReleaseSavepointPassesThroughSilently(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec('SAVEPOINT sp1');
        $result = $this->pdo->exec('RELEASE SAVEPOINT sp1');
        $this->assertSame(0, $result);
        $this->pdo->commit();
    }

    public function testRollbackToSavepointDoesNotUndoShadowData(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");

        $this->pdo->exec('SAVEPOINT sp1');
        $this->pdo->exec("INSERT INTO sp_test VALUES (3, 'Charlie')");
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Shadow store does NOT participate in savepoints — Charlie persists
        $this->assertSame(3, $cnt);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS sp_test');
    }
}
