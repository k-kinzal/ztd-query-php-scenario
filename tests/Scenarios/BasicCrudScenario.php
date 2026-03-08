<?php

declare(strict_types=1);

namespace Tests\Scenarios;

/**
 * Shared basic CRUD scenario for all platforms.
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

    public function testInsertAndSelect(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    public function testUpdateAndVerify(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("UPDATE users SET name = 'Alice Updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(1, $rows);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAndVerify(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("DELETE FROM users WHERE id = 1");

        $rows = $this->ztdQuery('SELECT * FROM users WHERE id = 1');

        $this->assertCount(0, $rows);
    }

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

    public function testAutoDetectsDriver(): void
    {
        $this->assertTrue($this->isZtdEnabled());
    }

    public function testPreparedSelectWithBindValue(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $rows = $this->ztdPrepareAndExecute('SELECT * FROM users WHERE id = ?', [1]);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testSelectReturnsEmptyWhenNoRows(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM users');
        $this->assertSame([], $rows);
    }

    public function testMultipleInserts(): void
    {
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->ztdExec("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $rows = $this->ztdQuery('SELECT * FROM users ORDER BY id');

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testEnableDisableToggle(): void
    {
        $this->assertTrue($this->isZtdEnabled());

        $this->disableZtd();
        $this->assertFalse($this->isZtdEnabled());

        $this->enableZtd();
        $this->assertTrue($this->isZtdEnabled());
    }
}
