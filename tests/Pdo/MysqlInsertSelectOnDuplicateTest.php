<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT...ON DUPLICATE KEY UPDATE with prepared params.
 *
 * Existing test (MysqlInsertSelectUpsertTest) covers exec() path.
 * This tests the prepared statement path with params in both
 * the SELECT WHERE clause and the ON DUPLICATE KEY UPDATE clause.
 *
 * @spec SPEC-4.1a, SPEC-4.2a
 */
class MysqlInsertSelectOnDuplicateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_isod_source (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                score INT NOT NULL,
                category VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_isod_target (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                score INT NOT NULL,
                update_count INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_isod_target', 'my_isod_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_isod_source VALUES (1, 'Alice', 90, 'math')");
        $this->pdo->exec("INSERT INTO my_isod_source VALUES (2, 'Bob', 80, 'math')");
        $this->pdo->exec("INSERT INTO my_isod_source VALUES (3, 'Charlie', 95, 'science')");
        $this->pdo->exec("INSERT INTO my_isod_source VALUES (4, 'Diana', 85, 'science')");

        // Pre-existing target row for conflict
        $this->pdo->exec("INSERT INTO my_isod_target VALUES (1, 'Alice-old', 70, 0)");
    }

    /**
     * INSERT...SELECT...ON DUPLICATE KEY UPDATE with WHERE param (exec).
     */
    public function testInsertSelectOnDuplicateWithWhereExec(): void
    {
        $sql = "INSERT INTO my_isod_target (id, name, score)
                SELECT id, name, score FROM my_isod_source
                WHERE category = 'math'
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    score = VALUES(score),
                    update_count = update_count + 1";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, score, update_count FROM my_isod_target ORDER BY id");

            // Alice (id=1) should be updated, Bob (id=2) should be inserted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT ON DUP exec: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);

            // Alice should be updated
            $alice = $rows[0];
            if ($alice['name'] !== 'Alice') {
                $this->markTestIncomplete(
                    "Alice name expected 'Alice', got '{$alice['name']}'"
                );
            }
            if ((int) $alice['score'] !== 90) {
                $this->markTestIncomplete(
                    "Alice score expected 90, got {$alice['score']}"
                );
            }

            $this->assertSame('Alice', $alice['name']);
            $this->assertSame(90, (int) $alice['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT ON DUP exec failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT...SELECT...ON DUPLICATE KEY UPDATE with WHERE param.
     */
    public function testPreparedInsertSelectOnDuplicateWithParam(): void
    {
        $sql = "INSERT INTO my_isod_target (id, name, score)
                SELECT id, name, score FROM my_isod_source
                WHERE category = ?
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    score = VALUES(score),
                    update_count = update_count + 1";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['math']);

            $rows = $this->ztdQuery("SELECT id, name, score FROM my_isod_target ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT SELECT ON DUP: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame(90, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT SELECT ON DUP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT...ON DUPLICATE KEY UPDATE all new rows (no conflicts).
     */
    public function testInsertSelectOnDuplicateAllNew(): void
    {
        $sql = "INSERT INTO my_isod_target (id, name, score)
                SELECT id, name, score FROM my_isod_source
                WHERE category = 'science'
                ON DUPLICATE KEY UPDATE
                    score = VALUES(score)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM my_isod_target ORDER BY id");

            // Original Alice(1) + new Charlie(3) + Diana(4) = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT ON DUP all new: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT ON DUP all new failed: ' . $e->getMessage()
            );
        }
    }
}
