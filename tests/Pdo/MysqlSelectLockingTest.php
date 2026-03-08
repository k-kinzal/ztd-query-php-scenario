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
 * Tests SELECT with locking clauses on MySQL PDO ZTD.
 *
 * MySQL supports: FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE.
 *
 * ZTD rewrites SELECT queries by prepending WITH CTEs. Locking clauses
 * are preserved and accepted by MySQL. The locking is effectively a no-op
 * since CTE-derived rows are not physical table rows.
 */
class MysqlSelectLockingTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_lock_test');
        $raw->exec('CREATE TABLE pdo_lock_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_lock_test (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_lock_test (id, name) VALUES (2, 'Bob')");
    }

    /**
     * SELECT ... FOR UPDATE succeeds but locking is no-op.
     */
    public function testSelectForUpdateSucceedsButNoOp(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pdo_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * SELECT ... LOCK IN SHARE MODE succeeds but no-op.
     */
    public function testSelectLockInShareModeSucceeds(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pdo_lock_test WHERE id = 1 LOCK IN SHARE MODE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * SELECT ... FOR SHARE (MySQL 8.0+) succeeds but no-op.
     */
    public function testSelectForShareSucceeds(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pdo_lock_test WHERE id = 1 FOR SHARE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * Physical isolation: locking queries with ZTD disabled.
     */
    public function testSelectForUpdateWithZtdDisabled(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pdo_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($stmt);

        $this->pdo->rollBack();
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_lock_test');
        } catch (\Exception $e) {
        }
    }
}
