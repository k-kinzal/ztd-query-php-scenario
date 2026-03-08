<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests SELECT with locking clauses (FOR UPDATE, LOCK IN SHARE MODE) on MySQLi ZTD.
 *
 * ZTD rewrites SELECT queries by prepending WITH CTEs to shadow table data.
 * Locking clauses are preserved in the rewritten SQL and accepted by MySQL.
 * The locking is a no-op since CTE-derived rows are not physical table rows,
 * meaning no actual row locks are acquired. This is important for user code
 * that uses FOR UPDATE — it won't error, but it won't actually lock anything.
 */
class SelectLockingTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_lock_test');
        $raw->query('CREATE TABLE mi_lock_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_lock_test (id, name) VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_lock_test (id, name) VALUES (2, 'Bob')");
    }

    /**
     * SELECT ... FOR UPDATE succeeds on MySQL ZTD but locking is a no-op.
     *
     * MySQL allows FOR UPDATE on CTE-derived queries. The shadow data
     * is returned correctly, but no physical row locks are acquired.
     */
    public function testSelectForUpdateSucceedsButNoOpLock(): void
    {
        $this->mysqli->begin_transaction();

        $result = $this->mysqli->query('SELECT * FROM mi_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($result);

        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);

        $this->mysqli->rollback();
    }

    /**
     * SELECT ... LOCK IN SHARE MODE succeeds on MySQL ZTD but no-op.
     */
    public function testSelectLockInShareModeSucceedsButNoOp(): void
    {
        $this->mysqli->begin_transaction();

        $result = $this->mysqli->query('SELECT * FROM mi_lock_test WHERE id = 1 LOCK IN SHARE MODE');
        $this->assertNotFalse($result);

        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);

        $this->mysqli->rollback();
    }

    /**
     * SELECT ... FOR SHARE (MySQL 8.0+) succeeds but no-op.
     */
    public function testSelectForShareSucceedsButNoOp(): void
    {
        $this->mysqli->begin_transaction();

        $result = $this->mysqli->query('SELECT * FROM mi_lock_test WHERE id = 1 FOR SHARE');
        $this->assertNotFalse($result);

        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);

        $this->mysqli->rollback();
    }

    /**
     * SELECT without locking clause works normally with ZTD.
     */
    public function testSelectWithoutLockingWorks(): void
    {
        $this->mysqli->begin_transaction();

        $result = $this->mysqli->query('SELECT * FROM mi_lock_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);

        $this->mysqli->rollback();
    }

    /**
     * After disabling ZTD, SELECT ... FOR UPDATE works on physical tables.
     */
    public function testSelectForUpdateWorksWithZtdDisabled(): void
    {
        $this->mysqli->disableZtd();
        $this->mysqli->begin_transaction();

        $result = $this->mysqli->query('SELECT * FROM mi_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($result);

        $this->mysqli->rollback();
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_lock_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
