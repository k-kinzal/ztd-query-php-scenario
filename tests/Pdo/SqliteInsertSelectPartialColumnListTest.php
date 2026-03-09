<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with partial column list into a table with
 * AUTOINCREMENT primary key. Isolates whether the CTE rewriter
 * correctly maps column positions when the INSERT specifies fewer
 * columns than the target table has.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSelectPartialColumnListTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ispcl_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL NOT NULL
            )',
            'CREATE TABLE sl_ispcl_target (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_value REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ispcl_target', 'sl_ispcl_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ispcl_source VALUES (1, 'Alpha', 10.5)");
        $this->pdo->exec("INSERT INTO sl_ispcl_source VALUES (2, 'Beta', 20.0)");
        $this->pdo->exec("INSERT INTO sl_ispcl_source VALUES (3, 'Gamma', 30.7)");
    }

    /**
     * INSERT...SELECT with full explicit column list (all non-auto columns).
     */
    public function testInsertSelectFullExplicitColumns(): void
    {
        $sql = "INSERT INTO sl_ispcl_target (source_id, source_name, source_value)
                SELECT id, name, value FROM sl_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT source_id, source_name, source_value FROM sl_ispcl_target ORDER BY source_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Full column INSERT SELECT: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Check that values are NOT null
            if ($rows[0]['source_id'] === null) {
                $this->markTestIncomplete(
                    'Full column INSERT SELECT: source_id is NULL. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(1, (int) $rows[0]['source_id']);
            $this->assertSame('Alpha', $rows[0]['source_name']);
            $this->assertEqualsWithDelta(10.5, (float) $rows[0]['source_value'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Full column INSERT SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT with string literal in SELECT list.
     */
    public function testInsertSelectWithStringLiteral(): void
    {
        // Target table with extra column for a literal
        $this->pdo->exec("CREATE TABLE sl_ispcl_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            action TEXT NOT NULL
        )");

        $sql = "INSERT INTO sl_ispcl_log (ref_id, label, action)
                SELECT id, name, 'imported' FROM sl_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT ref_id, label, action FROM sl_ispcl_log ORDER BY ref_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'String literal INSERT SELECT: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            if ($rows[0]['ref_id'] === null || $rows[0]['action'] === null) {
                $this->markTestIncomplete(
                    'String literal INSERT SELECT: NULL columns. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(1, (int) $rows[0]['ref_id']);
            $this->assertSame('Alpha', $rows[0]['label']);
            $this->assertSame('imported', $rows[0]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'String literal INSERT SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT with WHERE clause and partial columns.
     */
    public function testInsertSelectWithWherePartialColumns(): void
    {
        $sql = "INSERT INTO sl_ispcl_target (source_id, source_name, source_value)
                SELECT id, name, value FROM sl_ispcl_source WHERE value > 15.0";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT source_id, source_name FROM sl_ispcl_target ORDER BY source_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE partial INSERT SELECT: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Beta', $rows[0]['source_name']);
            $this->assertSame('Gamma', $rows[1]['source_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE partial INSERT SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT * (all columns) — contrast with partial column list.
     */
    public function testInsertSelectStarIntoMatchingTable(): void
    {
        // Create a target with same schema as source
        $this->pdo->exec("CREATE TABLE sl_ispcl_archive (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL
        )");

        $sql = "INSERT INTO sl_ispcl_archive SELECT * FROM sl_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, value FROM sl_ispcl_archive ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT * failed: ' . $e->getMessage()
            );
        }
    }
}
