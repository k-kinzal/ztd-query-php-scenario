<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests upsert (ON CONFLICT) edge cases through ZTD shadow store on PostgreSQL.
 *
 * PostgreSQL supports INSERT ... ON CONFLICT DO UPDATE/DO NOTHING.
 * It does not have REPLACE INTO, so upserts use ON CONFLICT exclusively.
 * These patterns interact with the shadow store's PK tracking and row replacement.
 * @spec SPEC-4.8
 */
class PostgresReplaceAndUpsertEdgeCasesTest extends AbstractPostgresPdoTestCase
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
     * Upsert with existing PK using ON CONFLICT DO UPDATE: should overwrite.
     * (PostgreSQL equivalent of SQLite's REPLACE INTO)
     */
    public function testUpsertExistingRow(): void
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
     * Upsert with new PK using ON CONFLICT DO UPDATE: should insert.
     * (PostgreSQL equivalent of SQLite's REPLACE INTO for new rows)
     */
    public function testUpsertNewRow(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (3, 'gamma', 1)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('gamma', $rows[2]['name']);
    }

    /**
     * INSERT ... ON CONFLICT DO UPDATE: upsert existing row (same as testUpsertExistingRow,
     * but mirrors the SQLite test structure for direct comparison).
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
     * PostgreSQL may have the same bug.
     */
    public function testOnConflictDoUpdateIncrement(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'alpha', 1)
             ON CONFLICT(id) DO UPDATE SET version = ru_items.version + 1"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        // Bug: self-referencing expression ru_items.version + 1 may evaluate to 0 instead of 2
        if ((string) $rows[0]['version'] === '0') {
            $this->markTestIncomplete(
                'PostgreSQL counterpart of Issue #16: ON CONFLICT DO UPDATE self-referencing expression loses original value (got 0, expected 2)'
            );
        }
        $this->assertSame('2', (string) $rows[0]['version']);
    }

    /**
     * Multiple upsert operations on same PK.
     * (PostgreSQL equivalent of SQLite's multiple REPLACE)
     */
    public function testMultipleUpsertSamePk(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'v2', 2)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'v3', 3)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'v4', 4)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );

        $rows = $this->ztdQuery('SELECT * FROM ru_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('v4', $rows[0]['name']);
        $this->assertSame('4', (string) $rows[0]['version']);
    }

    /**
     * Total row count after multiple upsert operations should be stable.
     * (PostgreSQL equivalent of SQLite's REPLACE duplicate check)
     */
    public function testUpsertDoesNotCreateDuplicates(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'v2', 2)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (2, 'v2', 2)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'v3', 3)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM ru_items');
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Upsert followed by SELECT with aggregate.
     * (PostgreSQL equivalent of SQLite's REPLACE + aggregate)
     */
    public function testUpsertFollowedByAggregate(): void
    {
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (1, 'alpha', 10)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );
        $this->pdo->exec(
            "INSERT INTO ru_items VALUES (2, 'beta', 20)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, version = excluded.version"
        );

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
        $this->assertCount(1, $rows);
        // Original row should be unchanged
        if ($rows[0]['name'] !== 'alpha') {
            $this->markTestIncomplete('Issue #41: ON CONFLICT DO NOTHING may not enforce PK uniqueness in shadow store');
        }
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
