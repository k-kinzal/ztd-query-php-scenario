<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared basic CRUD scenario for all platforms.
 *
 * @spec SPEC-1.2, SPEC-2.1, SPEC-2.2, SPEC-2.3, SPEC-3.1, SPEC-3.2, SPEC-4.1
 *
 * Requires table: users (id INT/INTEGER PRIMARY KEY, name VARCHAR/TEXT, email VARCHAR/TEXT)
 * Provided by the concrete test class via getTableDDL().
 */
trait BasicCrudScenario
{
    abstract protected function ztdExec(string $sql): int|false;
    abstract protected function ztdQuery(string $sql): array;
    abstract protected function ztdPrepareAndExecute(string $sql, array $params): array;
    abstract protected function disableZtd(): void;
    abstract protected function enableZtd(): void;
    abstract protected function isZtdEnabled(): bool;

    /** @spec SPEC-3.1, SPEC-4.1 */
    public function testInsertAndSelect(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    /** @spec SPEC-4.2 */
    public function testUpdateAndVerify(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("UPDATE users SET name = 'Alice Updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    /** @spec SPEC-4.3 */
    public function testDeleteAndVerify(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("DELETE FROM users WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(0, $rows);
    }

    /** @spec SPEC-2.2 */
    public function testZtdIsolation(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $rows = $this->ztdQuery('SELECT * FROM users');
        $this->assertCount(1, $rows);

        // Data NOT in physical table
        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT * FROM users');
        $this->assertCount(0, $rows);
        $this->enableZtd();
    }

    /** @spec SPEC-1.5, SPEC-2.1 */
    public function testAutoDetectsDriver(): void
    {
        $this->assertTrue($this->isZtdEnabled());
    }

    /** @spec SPEC-3.2 */
    public function testPreparedSelectWithBindValue(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $rows = $this->ztdPrepareAndExecute('SELECT * FROM users WHERE id = ?', [1]);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /** @spec SPEC-3.1 */
    public function testSelectReturnsEmptyWhenNoRows(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM users');
        $this->assertSame([], $rows);
    }

    /** @spec SPEC-4.1 */
    public function testMultipleInserts(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $rows = $this->ztdQuery('SELECT * FROM users ORDER BY id');

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /** @spec SPEC-2.3 */
    public function testEnableDisableToggle(): void
    {
        $this->assertTrue($this->isZtdEnabled());

        $this->disableZtd();
        $this->assertFalse($this->isZtdEnabled());

        $this->enableZtd();
        $this->assertTrue($this->isZtdEnabled());
    }
}
