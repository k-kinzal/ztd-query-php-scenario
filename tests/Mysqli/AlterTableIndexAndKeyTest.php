<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ALTER TABLE with INDEX, KEY, FOREIGN KEY, and UNIQUE constraints on MySQL ZTD.
 *
 * The AlterTableMutation treats FOREIGN KEY operations as metadata-only (no-ops).
 * INDEX and UNIQUE KEY are handled at the parser level.
 * These tests verify that ALTER TABLE with these operations doesn't break the
 * shadow store or cause unexpected exceptions.
 * @spec SPEC-5.1a
 */
class AlterTableIndexAndKeyTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_aik_test (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_aik_child', 'mi_aik_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_aik_test (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->mysqli->query("INSERT INTO mi_aik_test (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
    }

    /**
     * ADD FOREIGN KEY is treated as metadata-only in ZTD.
     */
    public function testAddForeignKeyIsNoOp(): void
    {
        $this->mysqli->query('ALTER TABLE mi_aik_test ADD FOREIGN KEY (score) REFERENCES mi_aik_test(id)');

        // Data should still be accessible
        $result = $this->mysqli->query('SELECT name FROM mi_aik_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * DROP FOREIGN KEY is treated as metadata-only in ZTD.
     */
    public function testDropForeignKeyIsNoOp(): void
    {
        // First add, then drop
        $this->mysqli->query('ALTER TABLE mi_aik_test ADD FOREIGN KEY fk_score (score) REFERENCES mi_aik_test(id)');
        $this->mysqli->query('ALTER TABLE mi_aik_test DROP FOREIGN KEY fk_score');

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_aik_test');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }

    /**
     * ALTER COLUMN SET DEFAULT throws UnsupportedSqlException.
     * The MySqlRewriter blocks this before AlterTableMutation runs.
     */
    public function testAlterColumnSetDefaultThrows(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Mysqli\ZtdMysqliException::class);
        $this->mysqli->query("ALTER TABLE mi_aik_test ALTER COLUMN score SET DEFAULT 0");
    }

    /**
     * ALTER COLUMN DROP DEFAULT throws UnsupportedSqlException.
     */
    public function testAlterColumnDropDefaultThrows(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Mysqli\ZtdMysqliException::class);
        $this->mysqli->query("ALTER TABLE mi_aik_test ALTER COLUMN score DROP DEFAULT");
    }

    /**
     * ADD INDEX throws UnsupportedSqlException.
     */
    public function testAddIndexThrows(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Mysqli\ZtdMysqliException::class);
        $this->mysqli->query('ALTER TABLE mi_aik_test ADD INDEX idx_name (name)');
    }

    /**
     * ADD UNIQUE KEY may or may not throw depending on phpmyadmin/sql-parser parsing.
     * When the option is detected as UNIQUE, it throws. Otherwise it may be treated
     * as a regular ADD and succeed silently.
     */
    public function testAddUniqueKey(): void
    {
        try {
            $this->mysqli->query('ALTER TABLE mi_aik_test ADD UNIQUE KEY uk_email (email)');
            // If it succeeds, data should still be accessible
            $result = $this->mysqli->query('SELECT email FROM mi_aik_test WHERE id = 1');
            $this->assertSame('alice@test.com', $result->fetch_assoc()['email']);
        } catch (\ZtdQuery\Adapter\Mysqli\ZtdMysqliException $e) {
            // Expected: unsupported
            $this->assertStringContainsString('Unsupported', $e->getMessage());
        }
    }

    /**
     * DROP INDEX throws UnsupportedSqlException.
     */
    public function testDropIndexThrows(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Mysqli\ZtdMysqliException::class);
        $this->mysqli->query('ALTER TABLE mi_aik_test DROP INDEX idx_name');
    }

    /**
     * Multiple ALTER operations in sequence on same table.
     */
    public function testMultipleAlterOperationsInSequence(): void
    {
        $this->mysqli->query('ALTER TABLE mi_aik_test ADD COLUMN extra VARCHAR(50)');
        $this->mysqli->query("UPDATE mi_aik_test SET extra = 'val1' WHERE id = 1");
        $this->mysqli->query('ALTER TABLE mi_aik_test MODIFY COLUMN extra TEXT');

        $result = $this->mysqli->query('SELECT extra FROM mi_aik_test WHERE id = 1');
        $this->assertSame('val1', $result->fetch_assoc()['extra']);
    }

    /**
     * Data remains accessible after ALTER TABLE operations.
     */
    public function testDataIntegrityAfterAlter(): void
    {
        $this->mysqli->query('ALTER TABLE mi_aik_test ADD COLUMN status VARCHAR(20)');
        $this->mysqli->query("UPDATE mi_aik_test SET status = 'active' WHERE id = 1");

        // Verify all original columns still accessible
        $result = $this->mysqli->query('SELECT id, name, email, score, status FROM mi_aik_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('alice@test.com', $row['email']);
        $this->assertEquals(90, $row['score']);
        $this->assertSame('active', $row['status']);
    }
}
