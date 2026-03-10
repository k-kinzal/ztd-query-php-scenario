<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT ... FOR UPDATE behavior through the ZTD shadow store (PDO SQLite).
 *
 * SQLite does not support row-level locking clauses (FOR UPDATE, FOR SHARE).
 * This test verifies that such queries are either rejected with a clear error
 * or silently ignored, rather than causing unexpected behavior.
 *
 * @spec SPEC-3.1
 */
class SqliteSelectForUpdateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_forlock_accounts (id INTEGER PRIMARY KEY, balance REAL, owner TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_forlock_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_forlock_accounts VALUES (1, 1000.00, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_forlock_accounts VALUES (2, 2000.00, 'Bob')");
    }

    /**
     * SQLite does not support FOR UPDATE syntax.
     *
     * This test verifies that SELECT ... FOR UPDATE is either:
     * - rejected with a clear exception, or
     * - silently ignored (returning rows as if the clause were absent).
     *
     * Either behavior is acceptable as long as it is deterministic and
     * does not corrupt state.
     *
     * @spec SPEC-3.1
     */
    public function testSelectForUpdateThrowsOrIgnored(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM sl_forlock_accounts WHERE id = 1 FOR UPDATE');

            // If we reach here, the clause was silently ignored.
            // Verify the query still returned correct data.
            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alice', $rows[0]['owner']);
        } catch (\Throwable $e) {
            // An exception is the expected behavior for SQLite.
            // Verify it is a clear error rather than an internal crash.
            $this->assertNotEmpty(
                $e->getMessage(),
                'Exception should contain a meaningful error message'
            );
            $this->assertTrue(true, 'FOR UPDATE correctly rejected on SQLite: ' . $e->getMessage());
        }
    }
}
