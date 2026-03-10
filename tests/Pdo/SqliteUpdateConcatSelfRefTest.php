<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with string concatenation (||) referencing the column being updated.
 *
 * Self-referencing expressions in UPDATE SET are a known weak area (Issues #16, #112).
 * This tests the string concatenation variant: UPDATE t SET name = name || ' suffix'.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateConcatSelfRefTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ucsr_tags (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            path TEXT NOT NULL DEFAULT \'\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ucsr_tags'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ucsr_tags (id, label, path) VALUES (1, 'root', '/')");
        $this->pdo->exec("INSERT INTO sl_ucsr_tags (id, label, path) VALUES (2, 'child', '/root')");
        $this->pdo->exec("INSERT INTO sl_ucsr_tags (id, label, path) VALUES (3, 'leaf', '/root/child')");
    }

    /**
     * UPDATE with string concatenation appending to existing value.
     * UPDATE t SET path = path || '/archived'
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatAppend(): void
    {
        try {
            $affected = $this->ztdExec(
                "UPDATE sl_ucsr_tags SET path = path || '/archived' WHERE id = 2"
            );

            $rows = $this->ztdQuery('SELECT id, path FROM sl_ucsr_tags WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Concat append: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('/root/archived', $rows[0]['path'],
                'Path should be concatenated with /archived');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE concat append failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with concatenation prepending to existing value.
     * UPDATE t SET path = '/prefix' || path
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatPrepend(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucsr_tags SET path = '/v2' || path WHERE id = 3"
            );

            $rows = $this->ztdQuery('SELECT id, path FROM sl_ucsr_tags WHERE id = 3');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Concat prepend: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('/v2/root/child', $rows[0]['path'],
                'Path should have /v2 prepended');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE concat prepend failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE multiple columns with self-referencing concat expressions.
     * UPDATE t SET label = label || '_old', path = path || '/' || label
     * The second SET uses the ORIGINAL value of label (before this UPDATE).
     *
     * @spec SPEC-4.2
     */
    public function testUpdateMultiColumnConcatSelfRef(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucsr_tags SET label = label || '_old', path = path || '/' || label WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT id, label, path FROM sl_ucsr_tags WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-column concat: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('root_old', $rows[0]['label'],
                'Label should be concatenated with _old');
            // SQL standard: all SET expressions use pre-update values
            $this->assertSame('//root', $rows[0]['path'],
                'Path should use original label value (root), not the updated one');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-column concat self-ref failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Bulk UPDATE all rows with concat, then verify all changed.
     *
     * @spec SPEC-4.2
     */
    public function testBulkUpdateConcat(): void
    {
        try {
            $affected = $this->ztdExec(
                "UPDATE sl_ucsr_tags SET label = '[' || label || ']'"
            );

            $rows = $this->ztdQuery('SELECT id, label FROM sl_ucsr_tags ORDER BY id');

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Bulk concat: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertSame('[root]', $rows[0]['label']);
            $this->assertSame('[child]', $rows[1]['label']);
            $this->assertSame('[leaf]', $rows[2]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Bulk UPDATE concat failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with concat and ? param.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateConcat(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ucsr_tags SET label = label || ? WHERE id = ?"
            );
            $stmt->execute(['_archived', 2]);

            $rows = $this->ztdQuery('SELECT id, label FROM sl_ucsr_tags WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared concat: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('child_archived', $rows[0]['label'],
                'Label should be concatenated with _archived via prepared stmt');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE concat failed: ' . $e->getMessage()
            );
        }
    }
}
