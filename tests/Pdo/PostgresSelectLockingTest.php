<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SELECT with locking clauses on PostgreSQL PDO ZTD.
 *
 * PostgreSQL supports: FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE.
 *
 * ZTD rewrites SELECT queries by prepending WITH CTEs. In PostgreSQL,
 * locking clauses are preserved and accepted — the locking is effectively
 * a no-op since CTE-derived rows are not physical table rows.
 *
 * This is important to document: user code with FOR UPDATE will run without
 * errors but will NOT actually acquire any row locks.
 * @spec SPEC-10.2.11
 */
class PostgresSelectLockingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_lock_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pg_lock_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_lock_test (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_lock_test (id, name) VALUES (2, 'Bob')");
    }

    /**
     * SELECT ... FOR UPDATE succeeds on PostgreSQL ZTD but locking is no-op.
     *
     * The CTE-rewritten query preserves the FOR UPDATE clause.
     * PostgreSQL accepts it but no physical rows are locked.
     */
    public function testSelectForUpdateSucceedsButNoOpLock(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * SELECT ... FOR SHARE succeeds on PostgreSQL ZTD but no-op.
     */
    public function testSelectForShareSucceedsButNoOp(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1 FOR SHARE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * SELECT ... FOR NO KEY UPDATE succeeds on PostgreSQL ZTD but no-op.
     */
    public function testSelectForNoKeyUpdateSucceedsButNoOp(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1 FOR NO KEY UPDATE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * SELECT ... FOR KEY SHARE succeeds on PostgreSQL ZTD but no-op.
     */
    public function testSelectForKeyShareSucceedsButNoOp(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1 FOR KEY SHARE');
        $this->assertNotFalse($stmt);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * Plain SELECT without locking works fine with ZTD.
     */
    public function testSelectWithoutLockingWorks(): void
    {
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $this->pdo->rollBack();
    }

    /**
     * After disabling ZTD, SELECT ... FOR UPDATE works on physical tables.
     */
    public function testSelectForUpdateWorksWithZtdDisabled(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->beginTransaction();

        $stmt = $this->pdo->query('SELECT * FROM pg_lock_test WHERE id = 1 FOR UPDATE');
        $this->assertNotFalse($stmt);

        $this->pdo->rollBack();
    }
}
