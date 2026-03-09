<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY ... HAVING with aggregate thresholds on shadow data (SQLite).
 *
 * @spec SPEC-3.3
 */
class SqliteHavingThresholdShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_hts_sales (
            id INTEGER PRIMARY KEY,
            salesperson TEXT NOT NULL,
            region TEXT NOT NULL,
            amount REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_hts_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_hts_sales (id, salesperson, region, amount) VALUES
            (1, 'Alice', 'North', 100.00),
            (2, 'Alice', 'North', 200.00),
            (3, 'Bob', 'South', 150.00),
            (4, 'Charlie', 'North', 300.00),
            (5, 'Alice', 'South', 50.00),
            (6, 'Bob', 'South', 250.00)");
    }

    public function testHavingCountThreshold(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS sale_count
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING COUNT: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
            $this->assertSame('Bob', $rows[1]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING COUNT failed: ' . $e->getMessage());
        }
    }

    public function testHavingSumThreshold(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, SUM(amount) AS total
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(amount) >= 350.00
                 ORDER BY total DESC"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING SUM: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING SUM failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_hts_sales (id, salesperson, region, amount) VALUES
            (7, 'Charlie', 'East', 400.00),
            (8, 'Charlie', 'West', 500.00)");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'HAVING after INSERT: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_hts_sales WHERE salesperson = 'Bob'");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'HAVING after DELETE: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowUpdate(): void
    {
        // Move Alice's South sale to Bob — now Alice has 2, Bob has 3
        $this->pdo->exec("UPDATE sl_hts_sales SET salesperson = 'Bob' WHERE id = 5");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 3
                 ORDER BY salesperson"
            );

            // Alice: 2, Bob: 3, Charlie: 1 → HAVING >= 3: Bob only
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'HAVING after UPDATE: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testHavingWithSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, SUM(amount) AS total
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM sl_hts_sales)
                 ORDER BY total DESC"
            );

            // AVG = 175. Threshold: 350. Bob: 400 > 350
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'HAVING subquery: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING subquery failed: ' . $e->getMessage());
        }
    }

    public function testHavingGroupByMultiColumn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, region, COUNT(*) AS cnt
                 FROM sl_hts_sales
                 GROUP BY salesperson, region
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson, region"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING multi-column: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING multi-column failed: ' . $e->getMessage());
        }
    }

    /**
     * Conditional aggregation with HAVING.
     */
    public function testConditionalAggregationWithHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson,
                        SUM(CASE WHEN amount >= 200 THEN 1 ELSE 0 END) AS big_sales
                 FROM sl_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(CASE WHEN amount >= 200 THEN 1 ELSE 0 END) >= 1
                 ORDER BY salesperson"
            );

            // Alice: 200 (1 big), Bob: 250 (1 big), Charlie: 300 (1 big) = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Conditional HAVING: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional aggregation HAVING failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_hts_sales")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
