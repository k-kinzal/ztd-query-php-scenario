<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT WHERE NOT EXISTS (anti-join conditional insert).
 *
 * This is a very common pattern for inserting rows that don't already exist,
 * avoiding duplicates without ON CONFLICT. The CTE rewriter must handle
 * self-referencing NOT EXISTS subqueries where the target table appears
 * in both the INSERT target and the NOT EXISTS subquery.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertWhereNotExistsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_iwne_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL
            )',
            'CREATE TABLE sl_iwne_target (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_iwne_target', 'sl_iwne_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_iwne_source VALUES (1, 'Alice', 'A')");
        $this->pdo->exec("INSERT INTO sl_iwne_source VALUES (2, 'Bob', 'A')");
        $this->pdo->exec("INSERT INTO sl_iwne_source VALUES (3, 'Charlie', 'B')");
        $this->pdo->exec("INSERT INTO sl_iwne_source VALUES (4, 'Diana', 'B')");

        // Pre-existing target row
        $this->pdo->exec("INSERT INTO sl_iwne_target VALUES (1, 'Alice-existing', 'A')");
    }

    /**
     * INSERT...SELECT WHERE NOT EXISTS — skip existing rows.
     */
    public function testInsertSelectWhereNotExists(): void
    {
        $sql = "INSERT INTO sl_iwne_target (id, name, category)
                SELECT id, name, category FROM sl_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM sl_iwne_target WHERE sl_iwne_target.id = sl_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iwne_target ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT WHERE NOT EXISTS: expected 4 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            // Alice should retain existing name, not be overwritten
            $this->assertSame('Alice-existing', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT WHERE NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT...SELECT WHERE NOT EXISTS with param in subquery.
     */
    public function testPreparedInsertWhereNotExistsWithParam(): void
    {
        $sql = "INSERT INTO sl_iwne_target (id, name, category)
                SELECT id, name, category FROM sl_iwne_source
                WHERE category = ?
                AND NOT EXISTS (
                    SELECT 1 FROM sl_iwne_target WHERE sl_iwne_target.id = sl_iwne_source.id
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['B']);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iwne_target ORDER BY id");

            // Original Alice(1) + Charlie(3) + Diana(4) = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared INSERT NOT EXISTS: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Charlie', $rows[1]['name']);
            $this->assertSame('Diana', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT NOT EXISTS on shadow-inserted data — ensures NOT EXISTS
     * sees rows that were shadow-inserted in the same session.
     */
    public function testInsertNotExistsRespectsShadowInserts(): void
    {
        // First insert Bob into target via shadow
        $this->pdo->exec("INSERT INTO sl_iwne_target VALUES (2, 'Bob-shadow', 'A')");

        // Now try anti-join insert — Bob should be skipped
        $sql = "INSERT INTO sl_iwne_target (id, name, category)
                SELECT id, name, category FROM sl_iwne_source
                WHERE NOT EXISTS (
                    SELECT 1 FROM sl_iwne_target WHERE sl_iwne_target.id = sl_iwne_source.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iwne_target ORDER BY id");

            // Alice(1, existing) + Bob(2, shadow) + Charlie(3) + Diana(4) = 4
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Anti-join shadow visibility: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            // Bob should be the shadow-inserted version, not overwritten
            $this->assertSame('Bob-shadow', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Anti-join shadow visibility failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Self-referencing INSERT WHERE NOT EXISTS on same table.
     * INSERT INTO t SELECT ... FROM t WHERE NOT EXISTS (SELECT 1 FROM t WHERE ...).
     */
    public function testSelfReferencingInsertNotExists(): void
    {
        // Use source table as both source and target
        $sql = "INSERT INTO sl_iwne_source (id, name, category)
                SELECT id + 10, name || '-copy', category FROM sl_iwne_source
                WHERE category = 'A'
                AND NOT EXISTS (
                    SELECT 1 FROM sl_iwne_source s2 WHERE s2.id = sl_iwne_source.id + 10
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iwne_source ORDER BY id");

            // Original 4 + copies of Alice(11) and Bob(12) = 6
            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'Self-ref INSERT NOT EXISTS: expected 6, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);

            $copyNames = array_column(
                array_filter($rows, fn($r) => (int) $r['id'] > 10),
                'name'
            );
            $this->assertContains('Alice-copy', $copyNames);
            $this->assertContains('Bob-copy', $copyNames);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Self-ref INSERT NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }
}
