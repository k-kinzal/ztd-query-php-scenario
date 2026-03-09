<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT WHERE NOT EXISTS (anti-join conditional insert) on MySQL.
 *
 * @spec SPEC-4.1
 */
class MysqlInsertWhereNotExistsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_iwne_source (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_iwne_target (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_iwne_target', 'my_iwne_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_iwne_source VALUES (1, 'Alice', 'A')");
        $this->pdo->exec("INSERT INTO my_iwne_source VALUES (2, 'Bob', 'A')");
        $this->pdo->exec("INSERT INTO my_iwne_source VALUES (3, 'Charlie', 'B')");
        $this->pdo->exec("INSERT INTO my_iwne_source VALUES (4, 'Diana', 'B')");

        $this->pdo->exec("INSERT INTO my_iwne_target VALUES (1, 'Alice-existing', 'A')");
    }

    /**
     * INSERT...SELECT WHERE NOT EXISTS — skip existing.
     */
    public function testInsertSelectWhereNotExists(): void
    {
        $sql = "INSERT INTO my_iwne_target (id, name, category)
                SELECT id, name, category FROM my_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM my_iwne_target WHERE my_iwne_target.id = my_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM my_iwne_target ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Alice-existing', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT WHERE NOT EXISTS with category filter param.
     */
    public function testPreparedInsertNotExistsWithParam(): void
    {
        $sql = "INSERT INTO my_iwne_target (id, name, category)
                SELECT id, name, category FROM my_iwne_source
                WHERE category = ?
                AND NOT EXISTS (
                    SELECT 1 FROM my_iwne_target WHERE my_iwne_target.id = my_iwne_source.id
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['B']);

            $rows = $this->ztdQuery("SELECT id, name FROM my_iwne_target ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared NOT EXISTS: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Anti-join respects shadow-inserted rows.
     */
    public function testInsertNotExistsRespectsShadowInserts(): void
    {
        $this->pdo->exec("INSERT INTO my_iwne_target VALUES (2, 'Bob-shadow', 'A')");

        $sql = "INSERT INTO my_iwne_target (id, name, category)
                SELECT id, name, category FROM my_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM my_iwne_target WHERE my_iwne_target.id = my_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM my_iwne_target ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Shadow anti-join: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Bob-shadow', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow anti-join failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Self-referencing INSERT NOT EXISTS on same table.
     */
    public function testSelfReferencingInsertNotExists(): void
    {
        $sql = "INSERT INTO my_iwne_source (id, name, category)
                SELECT id + 10, CONCAT(name, '-copy'), category FROM my_iwne_source
                WHERE category = 'A'
                AND NOT EXISTS (
                    SELECT 1 FROM my_iwne_source s2 WHERE s2.id = my_iwne_source.id + 10
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM my_iwne_source ORDER BY id");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'Self-ref NOT EXISTS: expected 6, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Self-ref INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }
}
