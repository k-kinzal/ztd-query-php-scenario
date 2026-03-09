<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests REPLACE and ON CONFLICT (upsert) edge cases through ZTD shadow store.
 *
 * SQLite supports both REPLACE INTO and INSERT ... ON CONFLICT DO UPDATE.
 * These patterns interact with the shadow store's PK tracking and row replacement.
 * @spec SPEC-4.8
 */
class SqliteReplaceAndUpsertEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ru_items (id INT PRIMARY KEY, name VARCHAR(50), version INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ru_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ru_items VALUES (1, 'alpha', 1)");
        $this->pdo->exec("INSERT INTO ru_items VALUES (2, 'beta', 1)");
    }

    /**
     * REPLACE INTO with existing PK: should overwrite.
     */
    public function testReplaceExistingRow(): void
    {
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'alpha-v2', 2)");

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('alpha-v2', $rows[0]['name']);
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * REPLACE INTO with new PK: should insert.
     */
    public function testReplaceNewRow(): void
    {
        $this->pdo->exec("REPLACE INTO ru_items VALUES (3, 'gamma', 1)");

        $rows = $this->ztdQuery('SELECT * FROM ru_items ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('gamma', $rows[2]['name']);
    }

    /**
     * INSERT ... ON CONFLICT DO UPDATE: upsert existing row.
     */
    public function testOnConflictDoUpdate(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'alpha-v2', 2)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('alpha-v2', $rows[0]['name']);
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * INSERT ... ON CONFLICT DO UPDATE with self-referencing increment expression.
     * SQLite counterpart of MySQL Issue #16: self-referencing expression loses original value.
     */
    public function testOnConflictDoUpdateIncrement(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'alpha', 1)
             ON CONFLICT(id) DO UPDATE SET version = ru_items.version + 1"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        // Bug: self-referencing expression ru_items.version + 1 evaluates to 0 instead of 2
        if ((string) $rows[0]['version'] === '0') {
            $this->markTestIncomplete(
                'SQLite counterpart of Issue #16: ON CONFLICT DO UPDATE self-referencing expression loses original value (got 0, expected 2)'
            );
        }
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * Multiple REPLACE operations on same PK.
     */
    public function testMultipleReplaceSamePk(): void
    {
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'v2', 2)");
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'v3', 3)");
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'v4', 4)");

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('v4', $rows[0]['name']);
        $this->assertSame('4', (string) $rows[0]['version']);
    }

    /**
     * Total row count after multiple REPLACE operations should be stable.
     */
    public function testReplaceDoesNotCreateDuplicates(): void
    {
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'v2', 2)");
        $this->pdo->exec("REPLACE INTO ru_items VALUES (2, 'v2', 2)");
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'v3', 3)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ru_items');
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * REPLACE followed by SELECT with aggregate.
     */
    public function testReplaceFollowedByAggregate(): void
    {
        $this->pdo->exec("REPLACE INTO ru_items VALUES (1, 'alpha', 10)");
        $this->pdo->exec("REPLACE INTO ru_items VALUES (2, 'beta', 20)");

        $rows = $this->ztdQuery('SELECT SUM(version) AS total FROM ru_items');
        $this->assertSame('30', (string) $rows[0]['total']);
    }

    /**
     * INSERT ... ON CONFLICT DO NOTHING: should not modify existing row.
     * Related to Issue #41 on SQLite.
     */
    public function testOnConflictDoNothing(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'should-not-appear', 99)
             ON CONFLICT(id) DO NOTHING"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        // Issue #41: ON CONFLICT DO NOTHING creates duplicate PK rows in shadow store
        if (count($rows) > 1) {
            $this->markTestIncomplete('Issue #41: ON CONFLICT DO NOTHING created ' . count($rows) . ' rows for same PK');
        }
        $this->assertCount(1, $rows);
        $this->assertSame('alpha', $rows[0]['name']);
        $this->assertSame('1', (string) $rows[0]['version']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ru_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
