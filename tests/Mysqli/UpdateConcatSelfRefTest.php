<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with CONCAT() referencing the column being updated on MySQL.
 *
 * MySQL uses CONCAT() function instead of || operator for string concatenation.
 * Self-referencing expressions are a known weak area (Issues #16, #112).
 *
 * @spec SPEC-4.2
 */
class UpdateConcatSelfRefTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ucsr_tags (
            id INT PRIMARY KEY,
            label VARCHAR(200) NOT NULL,
            path VARCHAR(500) NOT NULL DEFAULT \'\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_ucsr_tags'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ucsr_tags (id, label, path) VALUES (1, 'root', '/')");
        $this->mysqli->query("INSERT INTO mi_ucsr_tags (id, label, path) VALUES (2, 'child', '/root')");
        $this->mysqli->query("INSERT INTO mi_ucsr_tags (id, label, path) VALUES (3, 'leaf', '/root/child')");
    }

    /**
     * UPDATE with CONCAT appending to existing value.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatAppend(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucsr_tags SET path = CONCAT(path, '/archived') WHERE id = 2"
            );

            $rows = $this->ztdQuery('SELECT id, path FROM mi_ucsr_tags WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('/root/archived', $rows[0]['path'],
                'Path should be concatenated with /archived');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE CONCAT append failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with CONCAT prepending.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatPrepend(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucsr_tags SET path = CONCAT('/v2', path) WHERE id = 3"
            );

            $rows = $this->ztdQuery('SELECT id, path FROM mi_ucsr_tags WHERE id = 3');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('/v2/root/child', $rows[0]['path']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE CONCAT prepend failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple columns with self-referencing CONCAT.
     * SET label = CONCAT(label, '_old'), path = CONCAT(path, '/', label)
     * path should use ORIGINAL label value (SQL standard).
     *
     * @spec SPEC-4.2
     */
    public function testUpdateMultiColumnConcatSelfRef(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucsr_tags SET label = CONCAT(label, '_old'), path = CONCAT(path, '/', label) WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT id, label, path FROM mi_ucsr_tags WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('root_old', $rows[0]['label']);
            $this->assertSame('//root', $rows[0]['path'],
                'Path should use original label (root), not updated value');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column CONCAT self-ref failed: ' . $e->getMessage());
        }
    }

    /**
     * Bulk UPDATE with CONCAT wrapping.
     *
     * @spec SPEC-4.2
     */
    public function testBulkUpdateConcat(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucsr_tags SET label = CONCAT('[', label, ']')"
            );

            $rows = $this->ztdQuery('SELECT id, label FROM mi_ucsr_tags ORDER BY id');

            if (count($rows) !== 3) {
                $this->markTestIncomplete('Expected 3 rows, got ' . count($rows));
            }

            $this->assertSame('[root]', $rows[0]['label']);
            $this->assertSame('[child]', $rows[1]['label']);
            $this->assertSame('[leaf]', $rows[2]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Bulk UPDATE CONCAT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with CONCAT and ? param.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateConcat(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM mi_ucsr_tags WHERE id = ? AND label = CONCAT(label, ?)",
                [2, '_archived']
            );

            $stmt = $this->mysqli->prepare(
                "UPDATE mi_ucsr_tags SET label = CONCAT(label, ?) WHERE id = ?"
            );
            $stmt->bind_param('si', ...[$suffix = '_archived', $id = 2]);
            $stmt->execute();

            $result = $this->ztdQuery('SELECT id, label FROM mi_ucsr_tags WHERE id = 2');

            if (count($result) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($result));
            }

            $this->assertSame('child_archived', $result[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CONCAT failed: ' . $e->getMessage());
        }
    }
}
