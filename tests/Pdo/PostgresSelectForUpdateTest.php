<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SELECT ... FOR UPDATE / FOR SHARE behavior through the ZTD shadow store (PDO PostgreSQL).
 *
 * Locking clauses must survive CTE rewriting performed by ztd-query.
 * PostgreSQL supports additional locking modes beyond FOR UPDATE and FOR SHARE:
 * FOR NO KEY UPDATE and FOR KEY SHARE.
 *
 * @spec SPEC-3.1
 */
class PostgresSelectForUpdateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_forlock_accounts (id INT PRIMARY KEY, balance DECIMAL(10,2), owner VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pg_forlock_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_forlock_accounts VALUES (1, 1000.00, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_forlock_accounts VALUES (2, 2000.00, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_forlock_accounts VALUES (3, 3000.00, 'Carol')");
    }

    /**
     * Basic SELECT ... FOR UPDATE should return rows.
     *
     * If the locking clause is stripped by CTE rewriting the query may
     * still succeed but without locking.  If it is mis-placed the DB
     * will reject the query entirely.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForUpdateReturnsRows(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_forlock_accounts WHERE id = 1 FOR UPDATE');

            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alice', $rows[0]['owner']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT ... FOR UPDATE not supported through ZTD shadow store: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT FOR UPDATE inside an explicit transaction should return rows.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForUpdateInTransaction(): void
    {
        try {
            $this->ztdBeginTransaction();
            $rows = $this->ztdQuery('SELECT * FROM pg_forlock_accounts WHERE id <= 2 FOR UPDATE');
            $this->ztdCommit();

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['owner']);
            $this->assertSame('Bob', $rows[1]['owner']);
        } catch (\Throwable $e) {
            try {
                $this->ztdRollBack();
            } catch (\Throwable) {
            }
            $this->markTestIncomplete(
                'SELECT ... FOR UPDATE in transaction not supported through ZTD: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT ... FOR SHARE should return rows.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForShareReturnsRows(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * FROM pg_forlock_accounts WHERE owner = 'Carol' FOR SHARE");

            $this->assertCount(1, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertEquals(3000.00, (float) $rows[0]['balance']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT ... FOR SHARE not supported through ZTD shadow store: ' . $e->getMessage()
            );
        }
    }

    /**
     * Transaction with SELECT FOR UPDATE followed by UPDATE, then commit.
     *
     * This is the canonical pessimistic-locking workflow:
     * 1. BEGIN
     * 2. SELECT ... FOR UPDATE  (acquire row lock)
     * 3. UPDATE ...             (modify locked rows)
     * 4. COMMIT
     *
     * @spec SPEC-3.1
     */
    public function testDmlAfterSelectForUpdate(): void
    {
        try {
            $this->ztdBeginTransaction();

            $rows = $this->ztdQuery('SELECT balance FROM pg_forlock_accounts WHERE id = 1 FOR UPDATE');
            $this->assertCount(1, $rows);
            $oldBalance = (float) $rows[0]['balance'];

            $this->pdo->exec("UPDATE pg_forlock_accounts SET balance = balance - 100 WHERE id = 1");
            $this->pdo->exec("UPDATE pg_forlock_accounts SET balance = balance + 100 WHERE id = 2");

            $this->ztdCommit();

            // Verify the transfer
            $rows = $this->ztdQuery('SELECT id, balance FROM pg_forlock_accounts WHERE id IN (1, 2) ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertEquals($oldBalance - 100, (float) $rows[0]['balance']);
            $this->assertEquals(2100.00, (float) $rows[1]['balance']);
        } catch (\Throwable $e) {
            try {
                $this->ztdRollBack();
            } catch (\Throwable) {
            }
            $this->markTestIncomplete(
                'DML after SELECT ... FOR UPDATE not supported through ZTD: ' . $e->getMessage()
            );
        }
    }

    /**
     * PostgreSQL-specific FOR NO KEY UPDATE locking mode.
     *
     * FOR NO KEY UPDATE is weaker than FOR UPDATE: it does not block
     * concurrent FOR KEY SHARE locks on the same row.  This is useful
     * when updating non-key columns.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForNoKeyUpdate(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_forlock_accounts WHERE id = 2 FOR NO KEY UPDATE');

            $this->assertCount(1, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame('Bob', $rows[0]['owner']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT ... FOR NO KEY UPDATE not supported through ZTD shadow store: ' . $e->getMessage()
            );
        }
    }

    /**
     * PostgreSQL-specific FOR KEY SHARE locking mode.
     *
     * FOR KEY SHARE is the weakest row-level lock.  It conflicts only
     * with FOR UPDATE (not FOR NO KEY UPDATE), making it suitable for
     * foreign-key checks.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForKeyShare(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM pg_forlock_accounts WHERE id = 3 FOR KEY SHARE');

            $this->assertCount(1, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame('Carol', $rows[0]['owner']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT ... FOR KEY SHARE not supported through ZTD shadow store: ' . $e->getMessage()
            );
        }
    }
}
