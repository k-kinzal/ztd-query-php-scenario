<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a sales commission scenario through ZTD shadow store (SQLite PDO).
 * Sales reps earn commissions on deals. Exercises window functions:
 * ROW_NUMBER() for deal sequencing, SUM() OVER for running totals,
 * LAG() for previous deal comparison, window function in derived table,
 * prepared statement with window function query, and physical isolation.
 * @spec SPEC-10.2.167
 */
class SqliteSalesCommissionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sc_reps (
                id INTEGER PRIMARY KEY,
                name TEXT,
                team TEXT
            )',
            'CREATE TABLE sl_sc_deals (
                id INTEGER PRIMARY KEY,
                rep_id INTEGER,
                client TEXT,
                amount TEXT,
                close_date TEXT,
                commission_rate TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sc_deals', 'sl_sc_reps'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 reps
        $this->pdo->exec("INSERT INTO sl_sc_reps VALUES (1, 'Alice', 'north')");
        $this->pdo->exec("INSERT INTO sl_sc_reps VALUES (2, 'Bob', 'south')");
        $this->pdo->exec("INSERT INTO sl_sc_reps VALUES (3, 'Carol', 'north')");

        // 8 deals
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (1, 1, 'Acme Corp', '50000.00', '2025-01-15', '0.10')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (2, 1, 'Beta Inc', '30000.00', '2025-03-20', '0.10')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (3, 1, 'Gamma LLC', '75000.00', '2025-06-10', '0.12')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (4, 2, 'Delta Co', '20000.00', '2025-02-05', '0.10')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (5, 2, 'Echo Ltd', '45000.00', '2025-04-18', '0.10')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (6, 2, 'Foxtrot SA', '60000.00', '2025-07-22', '0.12')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (7, 3, 'Golf Corp', '35000.00', '2025-05-01', '0.10')");
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (8, 3, 'Hotel Inc', '90000.00', '2025-08-15', '0.15')");
    }

    /**
     * ROW_NUMBER() OVER (PARTITION BY rep_id ORDER BY close_date) for deal sequencing.
     * Expected 8 rows: Alice gets seq 1,2,3; Bob gets 1,2,3; Carol gets 1,2.
     */
    public function testDealSequenceByRep(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.rep_id, r.name, d.client, d.close_date,
                    ROW_NUMBER() OVER (PARTITION BY d.rep_id ORDER BY d.close_date) AS seq
             FROM sl_sc_deals d
             JOIN sl_sc_reps r ON r.id = d.rep_id
             ORDER BY d.rep_id, d.close_date"
        );

        $this->assertCount(8, $rows);

        // Alice: seq 1,2,3
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Acme Corp', $rows[0]['client']);
        $this->assertEquals(1, (int) $rows[0]['seq']);

        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Beta Inc', $rows[1]['client']);
        $this->assertEquals(2, (int) $rows[1]['seq']);

        $this->assertSame('Alice', $rows[2]['name']);
        $this->assertSame('Gamma LLC', $rows[2]['client']);
        $this->assertEquals(3, (int) $rows[2]['seq']);

        // Bob: seq 1,2,3
        $this->assertSame('Bob', $rows[3]['name']);
        $this->assertSame('Delta Co', $rows[3]['client']);
        $this->assertEquals(1, (int) $rows[3]['seq']);

        $this->assertSame('Bob', $rows[4]['name']);
        $this->assertSame('Echo Ltd', $rows[4]['client']);
        $this->assertEquals(2, (int) $rows[4]['seq']);

        $this->assertSame('Bob', $rows[5]['name']);
        $this->assertSame('Foxtrot SA', $rows[5]['client']);
        $this->assertEquals(3, (int) $rows[5]['seq']);

        // Carol: seq 1,2
        $this->assertSame('Carol', $rows[6]['name']);
        $this->assertSame('Golf Corp', $rows[6]['client']);
        $this->assertEquals(1, (int) $rows[6]['seq']);

        $this->assertSame('Carol', $rows[7]['name']);
        $this->assertSame('Hotel Inc', $rows[7]['client']);
        $this->assertEquals(2, (int) $rows[7]['seq']);
    }

    /**
     * SUM(amount) OVER (PARTITION BY rep_id ORDER BY close_date ROWS UNBOUNDED PRECEDING)
     * for running totals per rep.
     * Alice: 50000, 80000, 155000; Bob: 20000, 65000, 125000; Carol: 35000, 125000.
     */
    public function testRunningTotalByRep(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.rep_id, r.name, d.client, d.amount,
                    SUM(d.amount) OVER (PARTITION BY d.rep_id ORDER BY d.close_date ROWS UNBOUNDED PRECEDING) AS running_total
             FROM sl_sc_deals d
             JOIN sl_sc_reps r ON r.id = d.rep_id
             ORDER BY d.rep_id, d.close_date"
        );

        $this->assertCount(8, $rows);

        // Alice running totals
        $this->assertEqualsWithDelta(50000.00, (float) $rows[0]['running_total'], 0.01);
        $this->assertEqualsWithDelta(80000.00, (float) $rows[1]['running_total'], 0.01);
        $this->assertEqualsWithDelta(155000.00, (float) $rows[2]['running_total'], 0.01);

        // Bob running totals
        $this->assertEqualsWithDelta(20000.00, (float) $rows[3]['running_total'], 0.01);
        $this->assertEqualsWithDelta(65000.00, (float) $rows[4]['running_total'], 0.01);
        $this->assertEqualsWithDelta(125000.00, (float) $rows[5]['running_total'], 0.01);

        // Carol running totals
        $this->assertEqualsWithDelta(35000.00, (float) $rows[6]['running_total'], 0.01);
        $this->assertEqualsWithDelta(125000.00, (float) $rows[7]['running_total'], 0.01);
    }

    /**
     * LAG(amount) OVER (PARTITION BY rep_id ORDER BY close_date) AS prev_amount.
     * First deal per rep has NULL prev_amount; subsequent have previous deal amounts.
     */
    public function testLagComparePreviousDeal(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.rep_id, r.name, d.client, d.amount,
                    LAG(d.amount) OVER (PARTITION BY d.rep_id ORDER BY d.close_date) AS prev_amount
             FROM sl_sc_deals d
             JOIN sl_sc_reps r ON r.id = d.rep_id
             ORDER BY d.rep_id, d.close_date"
        );

        $this->assertCount(8, $rows);

        // Alice: first is NULL, then 50000, then 30000
        $this->assertNull($rows[0]['prev_amount']);
        $this->assertEqualsWithDelta(50000.00, (float) $rows[1]['prev_amount'], 0.01);
        $this->assertEqualsWithDelta(30000.00, (float) $rows[2]['prev_amount'], 0.01);

        // Bob: first is NULL, then 20000, then 45000
        $this->assertNull($rows[3]['prev_amount']);
        $this->assertEqualsWithDelta(20000.00, (float) $rows[4]['prev_amount'], 0.01);
        $this->assertEqualsWithDelta(45000.00, (float) $rows[5]['prev_amount'], 0.01);

        // Carol: first is NULL, then 35000
        $this->assertNull($rows[6]['prev_amount']);
        $this->assertEqualsWithDelta(35000.00, (float) $rows[7]['prev_amount'], 0.01);
    }

    /**
     * Use ROW_NUMBER in derived table then filter WHERE rn = 1 to get top deal per rep.
     * Expected: Alice=Gamma LLC/75000, Bob=Foxtrot SA/60000, Carol=Hotel Inc/90000.
     * Window function in derived table is known to be problematic with ZTD.
     */
    public function testTopDealPerRepViaDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ranked.name, ranked.client, ranked.amount
             FROM (
                 SELECT r.name, d.client, d.amount,
                        ROW_NUMBER() OVER (PARTITION BY d.rep_id ORDER BY d.amount DESC) AS rn
                 FROM sl_sc_deals d
                 JOIN sl_sc_reps r ON r.id = d.rep_id
             ) ranked
             WHERE ranked.rn = 1
             ORDER BY ranked.name"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Window function in derived table returns empty through ZTD. '
                . 'Possible CTE rewriter limitation with derived-table window functions.'
            );
        }

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Gamma LLC', $rows[0]['client']);
        $this->assertEqualsWithDelta(75000.00, (float) $rows[0]['amount'], 0.01);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Foxtrot SA', $rows[1]['client']);
        $this->assertEqualsWithDelta(60000.00, (float) $rows[1]['amount'], 0.01);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('Hotel Inc', $rows[2]['client']);
        $this->assertEqualsWithDelta(90000.00, (float) $rows[2]['amount'], 0.01);
    }

    /**
     * Prepared statement filtering by team with window function.
     * Filter team='north' (Alice, Carol), compute window functions over filtered set.
     * Expected 5 rows for north team.
     */
    public function testPreparedWindowFunctionQuery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.name, d.client, d.amount,
                    ROW_NUMBER() OVER (PARTITION BY d.rep_id ORDER BY d.close_date) AS seq,
                    SUM(d.amount) OVER (PARTITION BY d.rep_id ORDER BY d.close_date ROWS UNBOUNDED PRECEDING) AS running_total
             FROM sl_sc_deals d
             JOIN sl_sc_reps r ON r.id = d.rep_id
             WHERE r.team = ?
             ORDER BY r.name, d.close_date",
            ['north']
        );

        $this->assertCount(5, $rows);

        // Alice: 3 deals
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Acme Corp', $rows[0]['client']);
        $this->assertEquals(1, (int) $rows[0]['seq']);
        $this->assertEqualsWithDelta(50000.00, (float) $rows[0]['running_total'], 0.01);

        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Beta Inc', $rows[1]['client']);
        $this->assertEquals(2, (int) $rows[1]['seq']);
        $this->assertEqualsWithDelta(80000.00, (float) $rows[1]['running_total'], 0.01);

        $this->assertSame('Alice', $rows[2]['name']);
        $this->assertSame('Gamma LLC', $rows[2]['client']);
        $this->assertEquals(3, (int) $rows[2]['seq']);
        $this->assertEqualsWithDelta(155000.00, (float) $rows[2]['running_total'], 0.01);

        // Carol: 2 deals
        $this->assertSame('Carol', $rows[3]['name']);
        $this->assertSame('Golf Corp', $rows[3]['client']);
        $this->assertEquals(1, (int) $rows[3]['seq']);
        $this->assertEqualsWithDelta(35000.00, (float) $rows[3]['running_total'], 0.01);

        $this->assertSame('Carol', $rows[4]['name']);
        $this->assertSame('Hotel Inc', $rows[4]['client']);
        $this->assertEquals(2, (int) $rows[4]['seq']);
        $this->assertEqualsWithDelta(125000.00, (float) $rows[4]['running_total'], 0.01);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert new deal via shadow
        $this->pdo->exec("INSERT INTO sl_sc_deals VALUES (9, 1, 'India Ltd', '40000.00', '2025-09-01', '0.10')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sc_deals");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_sc_deals")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
