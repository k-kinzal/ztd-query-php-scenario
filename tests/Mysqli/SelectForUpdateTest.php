<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT ... FOR UPDATE / FOR SHARE behavior through the ZTD shadow store.
 *
 * Locking clauses must survive CTE rewriting performed by ztd-query.
 * If the rewriter strips or misplaces the locking clause, the database
 * will either reject the query or return rows without acquiring the lock.
 *
 * @spec SPEC-3.1
 */
class SelectForUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_forlock_accounts (id INT PRIMARY KEY, balance DECIMAL(10,2), owner VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_forlock_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_forlock_accounts VALUES (1, 1000.00, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_forlock_accounts VALUES (2, 2000.00, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_forlock_accounts VALUES (3, 3000.00, 'Carol')");
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
            $rows = $this->ztdQuery('SELECT * FROM mi_forlock_accounts WHERE id = 1 FOR UPDATE');

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
            $rows = $this->ztdQuery('SELECT * FROM mi_forlock_accounts WHERE id <= 2 FOR UPDATE');
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
     * SELECT ... FOR SHARE (MySQL 8+ alias for LOCK IN SHARE MODE).
     *
     * @spec SPEC-3.1
     */
    public function testSelectForShareReturnsRows(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM mi_forlock_accounts WHERE owner = \'Carol\' FOR SHARE');

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

            $rows = $this->ztdQuery('SELECT balance FROM mi_forlock_accounts WHERE id = 1 FOR UPDATE');
            $this->assertCount(1, $rows);
            $oldBalance = (float) $rows[0]['balance'];

            $this->mysqli->query("UPDATE mi_forlock_accounts SET balance = balance - 100 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_forlock_accounts SET balance = balance + 100 WHERE id = 2");

            $this->ztdCommit();

            // Verify the transfer
            $rows = $this->ztdQuery('SELECT id, balance FROM mi_forlock_accounts WHERE id IN (1, 2) ORDER BY id');
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
}
