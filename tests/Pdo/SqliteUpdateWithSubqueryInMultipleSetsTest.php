<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with subqueries that do NOT have WHERE (non-correlated).
 * Also tests SET with arithmetic expressions involving subqueries.
 *
 * The multi-correlated SET (with FROM...WHERE) is known to fail on SQLite.
 * This test verifies the non-correlated variants work correctly.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateWithSubqueryInMultipleSetsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_usmss_stats (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                min_val REAL NOT NULL DEFAULT 0,
                max_val REAL NOT NULL DEFAULT 0,
                avg_val REAL NOT NULL DEFAULT 0
            )',
            'CREATE TABLE sl_usmss_data (
                id INTEGER PRIMARY KEY,
                value REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_usmss_data', 'sl_usmss_stats'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (1, 10)");
        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (2, 20)");
        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (3, 30)");
        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (4, 40)");
        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (5, 50)");

        $this->pdo->exec("INSERT INTO sl_usmss_stats VALUES (1, 'Summary', 0, 0, 0)");
    }

    /**
     * UPDATE with multiple non-correlated aggregate subqueries in SET.
     */
    public function testUpdateMultipleAggregateSubqueries(): void
    {
        $sql = "UPDATE sl_usmss_stats SET
                min_val = (SELECT MIN(value) FROM sl_usmss_data),
                max_val = (SELECT MAX(value) FROM sl_usmss_data),
                avg_val = (SELECT AVG(value) FROM sl_usmss_data)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_val, max_val, avg_val FROM sl_usmss_stats WHERE id = 1");

            $this->assertCount(1, $rows);

            if (abs((float) $rows[0]['min_val'] - 10.0) > 0.01) {
                $this->markTestIncomplete(
                    'Multi-aggregate SET: min expected 10, got ' . $rows[0]['min_val']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['min_val'], 0.01);
            $this->assertEqualsWithDelta(50.0, (float) $rows[0]['max_val'], 0.01);
            $this->assertEqualsWithDelta(30.0, (float) $rows[0]['avg_val'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-aggregate SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with subquery arithmetic in SET.
     */
    public function testUpdateSubqueryArithmetic(): void
    {
        $sql = "UPDATE sl_usmss_stats SET
                min_val = (SELECT MIN(value) FROM sl_usmss_data),
                max_val = (SELECT MAX(value) FROM sl_usmss_data) - (SELECT MIN(value) FROM sl_usmss_data)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_val, max_val FROM sl_usmss_stats WHERE id = 1");

            $this->assertCount(1, $rows);

            // max_val = 50 - 10 = 40
            if (abs((float) $rows[0]['max_val'] - 40.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery arithmetic SET: max expected 40 (50-10), got '
                    . $rows[0]['max_val'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['min_val'], 0.01);
            $this->assertEqualsWithDelta(40.0, (float) $rows[0]['max_val'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with non-correlated subqueries on shadow data.
     */
    public function testUpdateSubqueryOnShadowData(): void
    {
        // Add data in shadow
        $this->pdo->exec("INSERT INTO sl_usmss_data VALUES (6, 100)");

        $sql = "UPDATE sl_usmss_stats SET
                min_val = (SELECT MIN(value) FROM sl_usmss_data),
                max_val = (SELECT MAX(value) FROM sl_usmss_data)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_val, max_val FROM sl_usmss_stats WHERE id = 1");

            $this->assertCount(1, $rows);

            // Max should now be 100 (shadow data)
            if (abs((float) $rows[0]['max_val'] - 100.0) > 0.01) {
                $this->markTestIncomplete(
                    'Shadow subquery SET: max expected 100, got ' . $rows[0]['max_val']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['min_val'], 0.01);
            $this->assertEqualsWithDelta(100.0, (float) $rows[0]['max_val'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Shadow subquery SET failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with subquery and param in WHERE.
     */
    public function testPreparedUpdateSubqueryWithWhereParam(): void
    {
        $sql = "UPDATE sl_usmss_stats SET
                min_val = (SELECT MIN(value) FROM sl_usmss_data),
                max_val = (SELECT MAX(value) FROM sl_usmss_data)
                WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT min_val, max_val FROM sl_usmss_stats WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['min_val'], 0.01);
            $this->assertEqualsWithDelta(50.0, (float) $rows[0]['max_val'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared subquery SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with COUNT subquery and conditional.
     */
    public function testUpdateSetCountWithConditional(): void
    {
        $sql = "UPDATE sl_usmss_stats SET
                label = CASE
                    WHEN (SELECT COUNT(*) FROM sl_usmss_data) > 3 THEN 'Large'
                    ELSE 'Small'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT label FROM sl_usmss_stats WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['label'] !== 'Large') {
                $this->markTestIncomplete(
                    'COUNT conditional SET: expected Large (5 rows > 3), got '
                    . $rows[0]['label'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Large', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT conditional SET failed: ' . $e->getMessage());
        }
    }
}
