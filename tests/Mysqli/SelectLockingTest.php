<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT with locking clauses (FOR UPDATE, LOCK IN SHARE MODE) on MySQLi ZTD.
 *
 * ZTD rewrites SELECT queries by prepending WITH CTEs to shadow table data.
 * Locking clauses are preserved in the rewritten SQL and accepted by MySQL.
 * The locking is a no-op since CTE-derived rows are not physical table rows,
 * meaning no actual row locks are acquired. This is important for user code
 * that uses FOR UPDATE — it won't error, but it won't actually lock anything.
 * @spec pending
 */
class SelectLockingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_lock_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_lock_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

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
}
