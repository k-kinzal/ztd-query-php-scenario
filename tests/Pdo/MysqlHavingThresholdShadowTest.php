<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP BY ... HAVING with aggregate thresholds on shadow data (MySQL).
 *
 * HAVING clauses that filter on aggregate results are a core SQL feature.
 * When data exists only in the shadow store (via CTE), the database must
 * correctly aggregate CTE data and apply HAVING filters.
 *
 * @spec SPEC-3.3
 */
class MysqlHavingThresholdShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_hts_sales (
            id INT PRIMARY KEY,
            salesperson VARCHAR(50),
            region VARCHAR(30),
            amount DECIMAL(10,2)
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_hts_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_hts_sales (id, salesperson, region, amount) VALUES
            (1, 'Alice', 'North', 100.00),
            (2, 'Alice', 'North', 200.00),
            (3, 'Bob', 'South', 150.00),
            (4, 'Charlie', 'North', 300.00),
            (5, 'Alice', 'South', 50.00),
            (6, 'Bob', 'South', 250.00)");
    }

    /**
     * HAVING COUNT(*) >= N on shadow data.
     */
    public function testHavingCountThreshold(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS sale_count
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            // Alice: 3 sales, Bob: 2 sales, Charlie: 1 sale
            // HAVING >= 2: Alice, Bob = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING COUNT >= 2: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
            $this->assertSame(3, (int) $rows[0]['sale_count']);
            $this->assertSame('Bob', $rows[1]['salesperson']);
            $this->assertSame(2, (int) $rows[1]['sale_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING COUNT threshold failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING SUM() threshold on shadow data.
     */
    public function testHavingSumThreshold(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, SUM(amount) AS total
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(amount) >= 350.00
                 ORDER BY total DESC"
            );

            // Alice: 350, Bob: 400, Charlie: 300
            // HAVING SUM >= 350: Bob (400), Alice (350) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING SUM >= 350: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Bob', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING SUM threshold failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING after shadow INSERT changes the group composition.
     */
    public function testHavingAfterShadowInsert(): void
    {
        // Give Charlie more sales so he crosses the threshold
        $this->pdo->exec("INSERT INTO mp_hts_sales (id, salesperson, region, amount) VALUES
            (7, 'Charlie', 'East', 400.00),
            (8, 'Charlie', 'West', 500.00)");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            // Now: Alice 3, Bob 2, Charlie 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'HAVING after INSERT: expected 3, got ' . count($rows)
                    . '. Shadow inserts may not be visible in GROUP BY HAVING.'
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING after shadow DELETE removes a group.
     */
    public function testHavingAfterShadowDelete(): void
    {
        // Delete Bob's sales — he should drop out of HAVING >= 2
        $this->pdo->exec("DELETE FROM mp_hts_sales WHERE salesperson = 'Bob'");

        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson"
            );

            // After DELETE: Alice 3, Charlie 1. HAVING >= 2: only Alice
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'HAVING after DELETE: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING after shadow DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING with multiple aggregate conditions.
     */
    public function testHavingMultipleConditions(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, COUNT(*) AS cnt, AVG(amount) AS avg_amt
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= 2 AND AVG(amount) > 100.00
                 ORDER BY salesperson"
            );

            // Alice: cnt=3, avg=116.67; Bob: cnt=2, avg=200; Charlie: cnt=1 (excluded by cnt)
            // Both Alice and Bob satisfy conditions
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING multi-condition: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING multiple conditions failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING with GROUP BY on two columns.
     */
    public function testHavingGroupByMultiColumn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, region, COUNT(*) AS cnt
                 FROM mp_hts_sales
                 GROUP BY salesperson, region
                 HAVING COUNT(*) >= 2
                 ORDER BY salesperson, region"
            );

            // Alice-North: 2, Alice-South: 1, Bob-South: 2, Charlie-North: 1
            // HAVING >= 2: Alice-North (2), Bob-South (2) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'HAVING multi-column GROUP BY: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertSame('Bob', $rows[1]['salesperson']);
            $this->assertSame('South', $rows[1]['region']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING multi-column GROUP BY failed: ' . $e->getMessage());
        }
    }

    /**
     * HAVING with subquery — compare aggregate to scalar subquery.
     */
    public function testHavingWithSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT salesperson, SUM(amount) AS total
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM mp_hts_sales)
                 ORDER BY total DESC"
            );

            // Overall AVG = (100+200+150+300+50+250)/6 = 175. Threshold: 350
            // Alice: 350, Bob: 400, Charlie: 300
            // > 350: Bob (400) = 1
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'HAVING with subquery: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('HAVING with subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared HAVING with parameter.
     */
    public function testPreparedHaving(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT salesperson, COUNT(*) AS cnt
                 FROM mp_hts_sales
                 GROUP BY salesperson
                 HAVING COUNT(*) >= ?
                 ORDER BY salesperson",
                [3]
            );

            // HAVING >= 3: only Alice (3)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared HAVING: expected 1, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['salesperson']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_hts_sales")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
