<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared transaction scenario for all platforms.
 *
 * @spec SPEC-4.8
 *
 * Requires table: tx_test (id INT/INTEGER PRIMARY KEY, val VARCHAR/TEXT)
 * Provided by the concrete test class via getTableDDL().
 *
 * The using class must implement beginTransaction(), commit(), rollBack()
 * which delegate to the appropriate adapter method.
 */
trait TransactionScenario
{
    abstract protected function ztdExec(string $sql): int|false;
    abstract protected function ztdQuery(string $sql): array;
    abstract protected function ztdBeginTransaction(): bool;
    abstract protected function ztdCommit(): bool;
    abstract protected function ztdRollBack(): bool;

    /** @spec SPEC-4.8 */
    public function testBeginTransactionAndCommit(): void
    {
        $this->assertTrue($this->ztdBeginTransaction());

        $this->ztdExec("INSERT INTO tx_test (id, val) VALUES (1, 'hello')");

        $this->assertTrue($this->ztdCommit());

        $rows = $this->ztdQuery('SELECT * FROM tx_test WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    /** @spec SPEC-4.8 */
    public function testBeginTransactionAndRollback(): void
    {
        $this->ztdExec("INSERT INTO tx_test (id, val) VALUES (1, 'before_tx')");

        $this->assertTrue($this->ztdBeginTransaction());
        $this->assertTrue($this->ztdRollBack());

        // Shadow data from before transaction should still be visible
        $rows = $this->ztdQuery('SELECT * FROM tx_test WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('before_tx', $rows[0]['val']);
    }

    /** @spec SPEC-4.8 */
    public function testDataVisibleAfterCommit(): void
    {
        $this->ztdBeginTransaction();
        $this->ztdExec("INSERT INTO tx_test (id, val) VALUES (1, 'committed')");
        $this->ztdExec("INSERT INTO tx_test (id, val) VALUES (2, 'also committed')");
        $this->ztdCommit();

        $rows = $this->ztdQuery('SELECT * FROM tx_test ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('committed', $rows[0]['val']);
        $this->assertSame('also committed', $rows[1]['val']);
    }
}
