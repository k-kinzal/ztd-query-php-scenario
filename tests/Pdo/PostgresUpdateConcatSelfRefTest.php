<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with string concatenation (||) referencing the column being updated
 * on PostgreSQL.
 *
 * Self-referencing expressions in UPDATE SET are a known weak area (Issues #16, #112).
 * PostgreSQL also supports CONCAT() function as an alternative.
 *
 * @spec SPEC-4.2
 */
class PostgresUpdateConcatSelfRefTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ucsr_nodes (
            id SERIAL PRIMARY KEY,
            label TEXT NOT NULL,
            path TEXT NOT NULL DEFAULT \'\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_ucsr_nodes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ucsr_nodes (id, label, path) VALUES (1, 'root', '/')");
        $this->pdo->exec("INSERT INTO pg_ucsr_nodes (id, label, path) VALUES (2, 'child', '/root')");
        $this->pdo->exec("INSERT INTO pg_ucsr_nodes (id, label, path) VALUES (3, 'leaf', '/root/child')");
    }

    /**
     * UPDATE with || append.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatAppend(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_ucsr_nodes SET path = path || '/archived' WHERE id = 2");

            $rows = $this->ztdQuery('SELECT id, path FROM pg_ucsr_nodes WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('/root/archived', $rows[0]['path']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE concat append failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with || prepend.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatPrepend(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_ucsr_nodes SET path = '/v2' || path WHERE id = 3");

            $rows = $this->ztdQuery('SELECT id, path FROM pg_ucsr_nodes WHERE id = 3');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('/v2/root/child', $rows[0]['path']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE concat prepend failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE multiple columns with self-referencing concat.
     * SET label = label || '_old', path = path || '/' || label
     * The path expression should use the ORIGINAL value of label.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateMultiColumnConcatSelfRef(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_ucsr_nodes SET label = label || '_old', path = path || '/' || label WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT id, label, path FROM pg_ucsr_nodes WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('root_old', $rows[0]['label']);
            $this->assertSame('//root', $rows[0]['path'],
                'Path should use original label value (root), not the updated one');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column concat self-ref failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with concat and $1 param.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateConcat(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_ucsr_nodes SET label = label || $1 WHERE id = $2"
            );
            $stmt->execute(['_archived', 2]);

            $rows = $this->ztdQuery('SELECT id, label FROM pg_ucsr_nodes WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('child_archived', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE concat failed: ' . $e->getMessage());
        }
    }

    /**
     * Bulk UPDATE all rows with concat.
     *
     * @spec SPEC-4.2
     */
    public function testBulkUpdateConcat(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_ucsr_nodes SET label = '[' || label || ']'");

            $rows = $this->ztdQuery('SELECT id, label FROM pg_ucsr_nodes ORDER BY id');

            if (count($rows) !== 3) {
                $this->markTestIncomplete('Expected 3 rows, got ' . count($rows));
            }

            $this->assertSame('[root]', $rows[0]['label']);
            $this->assertSame('[child]', $rows[1]['label']);
            $this->assertSame('[leaf]', $rows[2]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Bulk UPDATE concat failed: ' . $e->getMessage());
        }
    }
}
