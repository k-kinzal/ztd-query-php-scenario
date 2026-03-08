<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests SELECT with locking clauses on MySQL PDO ZTD.
 *
 * MySQL supports: FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE.
 *
 * ZTD rewrites SELECT queries by prepending WITH CTEs. Locking clauses
 * are preserved and accepted by MySQL. The locking is effectively a no-op
 * since CTE-derived rows are not physical table rows.
 * @spec pending
 */
class MysqlSelectLockingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_lock_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_lock_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

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
}
