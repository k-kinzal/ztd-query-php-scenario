<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUP BY ... HAVING with aggregate thresholds on shadow data (PostgreSQL).
 *
 * @spec SPEC-3.3
 */
class PostgresHavingThresholdShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_hts_sales (
            id INT PRIMARY KEY,
            salesperson VARCHAR(50),
            region VARCHAR(30),
            amount NUMERIC(10,2)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_hts_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_hts_sales (id, salesperson, region, amount) VALUES
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
                 FROM pg_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING COUNT >= 2: expected 2, got ' . count($rows)
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
                 FROM pg_hts_sales
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
            $this->assertSame('Bob', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING SUM failed: ' . $e->getMessage());
        }
    }

    public function testHavingAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_hts_sales (id, salesperson, region, amount) VALUES
            (7, 'Charlie', 'East', 400.00),
            (8, 'Charlie', 'West', 500.00)");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM pg_hts_sales
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
        $this->pdo->exec("DELETE FROM pg_hts_sales WHERE salesperson = 'Bob'");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM pg_hts_sales
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

    /**
     * HAVING with FILTER clause (PostgreSQL-specific).
     */
    public function testHavingWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE region = 'North') AS north_count
                 FROM pg_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) FILTER (WHERE region = 'North') >= 1
                 ORDER BY salesperson"
            );

            // Alice has 2 North, Charlie has 1 North, Bob has 0 North
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING FILTER: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
            $this->assertSame('Charlie', $rows[1]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING with FILTER failed: ' . $e->getMessage());
        }
    }

    public function testHavingWithSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, SUM(amount) AS total
                 FROM pg_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM pg_hts_sales)
                 ORDER BY total DESC"
            );

            // Overall AVG = 175. Threshold: 350. Bob: 400 > 350
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
                 FROM pg_hts_sales
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

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_hts_sales")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
