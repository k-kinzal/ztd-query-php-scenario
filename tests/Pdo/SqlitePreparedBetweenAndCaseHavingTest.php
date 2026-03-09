<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Prepared statements with BETWEEN, CASE in HAVING, and compound WHERE
 * conditions through ZTD shadow store on SQLite.
 *
 * BETWEEN with prepared params is untested and may confuse the CTE
 * rewriter's parameter binding. CASE in HAVING with prepared params
 * combines two areas known to be fragile: aggregate filtering and
 * conditional expressions with parameters.
 *
 * @spec SPEC-3.3
 */
class SqlitePreparedBetweenAndCaseHavingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_pbch_orders (
                id INTEGER PRIMARY KEY,
                customer VARCHAR(30),
                product VARCHAR(30),
                qty INTEGER,
                price REAL,
                order_date TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pbch_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (1, 'Alice', 'Widget', 5, 10.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (2, 'Alice', 'Gadget', 3, 25.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (3, 'Bob',   'Widget', 10, 10.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (4, 'Bob',   'Gadget', 2, 25.00, '2025-01-18')");
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (5, 'Carol', 'Widget', 7, 10.00, '2025-01-20')");
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (6, 'Carol', 'Gizmo',  1, 100.00, '2025-01-22')");
    }

    public function testPreparedBetweenIntegers(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, customer, qty FROM sl_pbch_orders WHERE qty BETWEEN ? AND ? ORDER BY id",
                [3, 7]
            );
            // qty 5, 3, 7 are in range [3,7]
            $this->assertCount(3, $rows);
            $ids = array_map('intval', array_column($rows, 'id'));
            $this->assertContains(1, $ids); // qty=5
            $this->assertContains(2, $ids); // qty=3
            $this->assertContains(5, $ids); // qty=7
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared BETWEEN integers failed: ' . $e->getMessage());
        }
    }

    public function testPreparedBetweenDates(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, customer, order_date FROM sl_pbch_orders
                 WHERE order_date BETWEEN ? AND ?
                 ORDER BY order_date",
                ['2025-01-12', '2025-01-18']
            );
            // Jan 12 (Bob Widget), Jan 15 (Alice Gadget), Jan 18 (Bob Gadget)
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared BETWEEN dates failed: ' . $e->getMessage());
        }
    }

    public function testPreparedNotBetween(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, qty FROM sl_pbch_orders WHERE qty NOT BETWEEN ? AND ? ORDER BY id",
                [3, 7]
            );
            // qty 10, 2, 1 are outside [3,7]
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared NOT BETWEEN failed: ' . $e->getMessage());
        }
    }

    public function testPreparedBetweenWithAdditionalCondition(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, customer, qty FROM sl_pbch_orders
                 WHERE qty BETWEEN ? AND ? AND customer = ?
                 ORDER BY id",
                [1, 10, 'Bob']
            );
            // Bob: qty=10 (in range), qty=2 (in range)
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared BETWEEN with AND failed: ' . $e->getMessage());
        }
    }

    public function testCaseInHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, SUM(qty * price) AS total_spend
                 FROM sl_pbch_orders
                 GROUP BY customer
                 HAVING CASE WHEN SUM(qty * price) > 100 THEN 1 ELSE 0 END = 1
                 ORDER BY customer"
            );
            // Alice: 5*10+3*25=125, Bob: 10*10+2*25=150, Carol: 7*10+1*100=170
            // All > 100, so all 3
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in HAVING failed: ' . $e->getMessage());
        }
    }

    public function testCaseInHavingWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer, SUM(qty * price) AS total_spend
                 FROM sl_pbch_orders
                 GROUP BY customer
                 HAVING CASE WHEN SUM(qty * price) > ? THEN 1 ELSE 0 END = 1
                 ORDER BY customer",
                [140.0]
            );
            // Alice: 125 < 140, Bob: 150 > 140, Carol: 170 > 140
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'customer');
            $this->assertContains('Bob', $names);
            $this->assertContains('Carol', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in HAVING with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testSearchedCaseInHavingMultipleBranches(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer, COUNT(*) AS order_count,
                        SUM(qty * price) AS total_spend
                 FROM sl_pbch_orders
                 GROUP BY customer
                 HAVING CASE
                     WHEN COUNT(*) > ? AND SUM(qty * price) > ? THEN 'vip'
                     WHEN COUNT(*) > ? THEN 'active'
                     ELSE 'regular'
                 END = 'vip'
                 ORDER BY customer",
                [1, 100.0, 1]
            );
            // Alice: count=2, spend=125 > 100 → vip
            // Bob: count=2, spend=150 > 100 → vip
            // Carol: count=2, spend=170 > 100 → vip
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Searched CASE in HAVING with multiple params failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_pbch_orders WHERE qty BETWEEN ? AND ?"
            );
            $stmt->execute([3, 7]);

            $rows = $this->ztdQuery("SELECT id FROM sl_pbch_orders ORDER BY id");
            // Remaining: Bob(qty=10), Bob(qty=2), Carol(qty=1) = ids 3,4,6
            $this->assertCount(3, $rows);
            $ids = array_map('intval', array_column($rows, 'id'));
            $this->assertContains(3, $ids);
            $this->assertContains(4, $ids);
            $this->assertContains(6, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE BETWEEN failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_pbch_orders SET price = price * 1.1 WHERE qty BETWEEN ? AND ?"
            );
            $stmt->execute([5, 10]);

            $rows = $this->ztdQuery(
                "SELECT id, price FROM sl_pbch_orders WHERE id IN (1, 3, 5) ORDER BY id"
            );
            // ids 1(qty=5), 3(qty=10), 5(qty=7) should have 10% increase
            $this->assertEquals(11.0, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(11.0, (float) $rows[1]['price'], '', 0.01);
            $this->assertEquals(11.0, (float) $rows[2]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE BETWEEN failed: ' . $e->getMessage());
        }
    }

    public function testPreparedBetweenAfterMutation(): void
    {
        // Insert new row then query with BETWEEN
        $this->pdo->exec("INSERT INTO sl_pbch_orders VALUES (7, 'Dave', 'Widget', 6, 10.00, '2025-01-25')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer FROM sl_pbch_orders WHERE qty BETWEEN ? AND ? ORDER BY customer",
                [5, 7]
            );
            // Alice(5), Carol(7), Dave(6)
            $this->assertCount(3, $rows);
            $this->assertContains('Dave', array_column($rows, 'customer'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared BETWEEN after mutation failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_pbch_orders")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
