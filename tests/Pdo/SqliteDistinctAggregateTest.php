<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DISTINCT inside aggregate functions through CTE shadow store.
 *
 * COUNT(DISTINCT col) is widely tested, but SUM(DISTINCT), AVG(DISTINCT),
 * and GROUP_CONCAT(DISTINCT) interact differently with the CTE rewriter.
 * This test covers a realistic sales commission scenario with duplicate
 * values that must be deduplicated before aggregation.
 *
 * @spec SPEC-3.3
 */
class SqliteDistinctAggregateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_da_reps (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL
            )',
            'CREATE TABLE sl_da_sales (
                id INTEGER PRIMARY KEY,
                rep_id INTEGER NOT NULL,
                product TEXT NOT NULL,
                amount REAL NOT NULL,
                sale_date TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_da_sales', 'sl_da_reps'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_da_reps VALUES (1, 'Alice', 'East')");
        $this->pdo->exec("INSERT INTO sl_da_reps VALUES (2, 'Bob', 'East')");
        $this->pdo->exec("INSERT INTO sl_da_reps VALUES (3, 'Carol', 'West')");

        // Alice: sells Widget (100), Widget (100), Gadget (200), Gadget (200)
        // Distinct amounts: 100, 200 → SUM(DISTINCT) = 300
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (1, 1, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (2, 1, 'Widget', 100.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (3, 1, 'Gadget', 200.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (4, 1, 'Gadget', 200.00, '2025-01-13')");

        // Bob: sells Widget (100), Gadget (200), Gizmo (150)
        // Distinct amounts: 100, 150, 200 → SUM(DISTINCT) = 450
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (5, 2, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (6, 2, 'Gadget', 200.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (7, 2, 'Gizmo', 150.00, '2025-01-12')");

        // Carol: sells Widget (100), Widget (100), Widget (100)
        // Distinct amounts: 100 → SUM(DISTINCT) = 100
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (8, 3, 'Widget', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (9, 3, 'Widget', 100.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_da_sales VALUES (10, 3, 'Widget', 100.00, '2025-01-12')");
    }

    /**
     * COUNT(DISTINCT product) per rep.
     */
    public function testCountDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(DISTINCT s.product) AS unique_products
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['unique_products']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['unique_products']);
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['unique_products']);
    }

    /**
     * SUM(DISTINCT amount) per rep — deduplicates amounts before summing.
     */
    public function testSumDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, SUM(DISTINCT s.amount) AS distinct_total
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(450.00, (float) $rows[1]['distinct_total']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(100.00, (float) $rows[2]['distinct_total']);
    }

    /**
     * AVG(DISTINCT amount) per rep.
     */
    public function testAvgDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, AVG(DISTINCT s.amount) AS avg_distinct
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        // Alice: AVG(100, 200) = 150
        $this->assertEquals(150.00, (float) $rows[0]['avg_distinct']);
        // Bob: AVG(100, 150, 200) = 150
        $this->assertEquals(150.00, (float) $rows[1]['avg_distinct']);
        // Carol: AVG(100) = 100
        $this->assertEquals(100.00, (float) $rows[2]['avg_distinct']);
    }

    /**
     * GROUP_CONCAT(DISTINCT product) per rep (SQLite).
     */
    public function testGroupConcatDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, GROUP_CONCAT(DISTINCT s.product) AS products
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);

        // Alice: Gadget, Widget (or Widget, Gadget — order not guaranteed)
        $aliceProducts = explode(',', $rows[0]['products']);
        sort($aliceProducts);
        $this->assertSame(['Gadget', 'Widget'], $aliceProducts);

        // Carol: just Widget
        $this->assertSame('Widget', $rows[2]['products']);
    }

    /**
     * SUM(DISTINCT) vs SUM() — verify they differ.
     */
    public function testSumDistinctVsSum(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name,
                    SUM(s.amount) AS total,
                    SUM(DISTINCT s.amount) AS distinct_total
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);

        // Alice: SUM = 600 (100+100+200+200), SUM(DISTINCT) = 300
        $this->assertEquals(600.00, (float) $rows[0]['total']);
        $this->assertEquals(300.00, (float) $rows[0]['distinct_total']);

        // Bob: SUM = 450, SUM(DISTINCT) = 450 (all unique)
        $this->assertEquals(450.00, (float) $rows[1]['total']);
        $this->assertEquals(450.00, (float) $rows[1]['distinct_total']);

        // Carol: SUM = 300, SUM(DISTINCT) = 100
        $this->assertEquals(300.00, (float) $rows[2]['total']);
        $this->assertEquals(100.00, (float) $rows[2]['distinct_total']);
    }

    /**
     * COUNT(DISTINCT) with HAVING filter.
     */
    public function testCountDistinctWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(DISTINCT s.product) AS unique_products
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.name
             HAVING COUNT(DISTINCT s.product) >= 2
             ORDER BY r.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * DISTINCT aggregate per region (multi-rep aggregation).
     */
    public function testDistinctAggregateByRegion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.region,
                    COUNT(DISTINCT s.product) AS unique_products,
                    SUM(DISTINCT s.amount) AS distinct_amounts
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             GROUP BY r.region
             ORDER BY r.region"
        );

        $this->assertCount(2, $rows);
        // East: Alice+Bob → products: Widget, Gadget, Gizmo → 3 unique
        // East amounts: 100, 200, 150 → distinct sum = 450
        $this->assertSame('East', $rows[0]['region']);
        $this->assertEquals(3, (int) $rows[0]['unique_products']);
        $this->assertEquals(450.00, (float) $rows[0]['distinct_amounts']);

        // West: Carol → products: Widget → 1 unique
        $this->assertSame('West', $rows[1]['region']);
        $this->assertEquals(1, (int) $rows[1]['unique_products']);
        $this->assertEquals(100.00, (float) $rows[1]['distinct_amounts']);
    }

    /**
     * Prepared statement with COUNT(DISTINCT).
     */
    public function testPreparedCountDistinct(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(DISTINCT s.product) AS unique_products
             FROM sl_da_sales s
             JOIN sl_da_reps r ON r.id = s.rep_id
             WHERE r.region = ?",
            ['East']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['unique_products']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_da_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
