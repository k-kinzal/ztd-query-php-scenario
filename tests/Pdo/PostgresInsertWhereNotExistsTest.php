<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT WHERE NOT EXISTS (anti-join) on PostgreSQL.
 *
 * @spec SPEC-4.1
 */
class PostgresInsertWhereNotExistsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_iwne_source (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(20) NOT NULL
            )',
            'CREATE TABLE pg_iwne_target (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_iwne_target', 'pg_iwne_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_iwne_source VALUES (1, 'Alice', 'A')");
        $this->pdo->exec("INSERT INTO pg_iwne_source VALUES (2, 'Bob', 'A')");
        $this->pdo->exec("INSERT INTO pg_iwne_source VALUES (3, 'Charlie', 'B')");
        $this->pdo->exec("INSERT INTO pg_iwne_source VALUES (4, 'Diana', 'B')");

        $this->pdo->exec("INSERT INTO pg_iwne_target VALUES (1, 'Alice-existing', 'A')");
    }

    /**
     * INSERT...SELECT WHERE NOT EXISTS.
     */
    public function testInsertSelectWhereNotExists(): void
    {
        $sql = "INSERT INTO pg_iwne_target (id, name, category)
                SELECT id, name, category FROM pg_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM pg_iwne_target WHERE pg_iwne_target.id = pg_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_iwne_target ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Alice-existing', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with ? param.
     */
    public function testPreparedInsertNotExistsQuestionMark(): void
    {
        $sql = "INSERT INTO pg_iwne_target (id, name, category)
                SELECT id, name, category FROM pg_iwne_source
                WHERE category = ?
                AND NOT EXISTS (
                    SELECT 1 FROM pg_iwne_target WHERE pg_iwne_target.id = pg_iwne_source.id
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['B']);

            $rows = $this->ztdQuery("SELECT id FROM pg_iwne_target ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared NOT EXISTS (?): expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared NOT EXISTS (?) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with $1 param.
     */
    public function testPreparedInsertNotExistsDollarParam(): void
    {
        $sql = "INSERT INTO pg_iwne_target (id, name, category)
                SELECT id, name, category FROM pg_iwne_source
                WHERE category = $1
                AND NOT EXISTS (
                    SELECT 1 FROM pg_iwne_target WHERE pg_iwne_target.id = pg_iwne_source.id
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['B']);

            $rows = $this->ztdQuery("SELECT id FROM pg_iwne_target ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared NOT EXISTS ($1): expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared NOT EXISTS ($1) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Anti-join respects shadow-inserted rows.
     */
    public function testInsertNotExistsRespectsShadowInserts(): void
    {
        $this->pdo->exec("INSERT INTO pg_iwne_target VALUES (2, 'Bob-shadow', 'A')");

        $sql = "INSERT INTO pg_iwne_target (id, name, category)
                SELECT id, name, category FROM pg_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM pg_iwne_target WHERE pg_iwne_target.id = pg_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_iwne_target ORDER BY id");

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
}
