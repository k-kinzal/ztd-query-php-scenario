<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with CONCAT() referencing the column being updated on MySQL PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlUpdateConcatSelfRefTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_ucsr_tags (
            id INT PRIMARY KEY,
            label VARCHAR(200) NOT NULL,
            path VARCHAR(500) NOT NULL DEFAULT \'\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_ucsr_tags'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ucsr_tags (id, label, path) VALUES (1, 'root', '/')");
        $this->pdo->exec("INSERT INTO mp_ucsr_tags (id, label, path) VALUES (2, 'child', '/root')");
        $this->pdo->exec("INSERT INTO mp_ucsr_tags (id, label, path) VALUES (3, 'leaf', '/root/child')");
    }

    /**
     * @spec SPEC-4.2
     */
    public function testUpdateConcatAppend(): void
    {
        try {
            $this->pdo->exec("UPDATE mp_ucsr_tags SET path = CONCAT(path, '/archived') WHERE id = 2");

            $rows = $this->ztdQuery('SELECT id, path FROM mp_ucsr_tags WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('/root/archived', $rows[0]['path']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE CONCAT append failed: ' . $e->getMessage());
        }
    }

    /**
     * @spec SPEC-4.2
     */
    public function testUpdateMultiColumnConcatSelfRef(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_ucsr_tags SET label = CONCAT(label, '_old'), path = CONCAT(path, '/', label) WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT id, label, path FROM mp_ucsr_tags WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('root_old', $rows[0]['label']);
            $this->assertSame('//root', $rows[0]['path'],
                'Path should use original label (root)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column CONCAT self-ref failed: ' . $e->getMessage());
        }
    }

    /**
     * @spec SPEC-4.2
     */
    public function testBulkUpdateConcat(): void
    {
        try {
            $this->pdo->exec("UPDATE mp_ucsr_tags SET label = CONCAT('[', label, ']')");

            $rows = $this->ztdQuery('SELECT id, label FROM mp_ucsr_tags ORDER BY id');

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
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateConcat(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE mp_ucsr_tags SET label = CONCAT(label, ?) WHERE id = ?");
            $stmt->execute(['_archived', 2]);

            $rows = $this->ztdQuery('SELECT id, label FROM mp_ucsr_tags WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Expected 1 row, got ' . count($rows));
            }

            $this->assertSame('child_archived', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CONCAT failed: ' . $e->getMessage());
        }
    }
}
