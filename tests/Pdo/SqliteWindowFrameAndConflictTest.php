<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests window functions with FRAME clauses and INSERT ON CONFLICT behavior on SQLite.
 * Window frames (ROWS/RANGE BETWEEN) are an advanced SQL feature.
 * INSERT ON CONFLICT DO NOTHING on SQLite is documented as broken (inserts both rows).
 */
class SqliteWindowFrameAndConflictTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sales (id INTEGER PRIMARY KEY, month TEXT, amount REAL)');
        $raw->exec('CREATE TABLE unique_items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testWindowFunctionRowsBetween(): void
    {
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (3, '2024-03', 150)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (4, '2024-04', 300)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (5, '2024-05', 250)");

        // 3-month rolling average using ROWS BETWEEN
        $stmt = $this->pdo->query("
            SELECT month, amount,
                   AVG(amount) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_avg
            FROM sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);

        // First row: avg of (100, 200) = 150
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['rolling_avg'], 0.01);
        // Second row: avg of (100, 200, 150) = 150
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['rolling_avg'], 0.01);
        // Third row: avg of (200, 150, 300) = 216.67
        $this->assertEqualsWithDelta(216.67, (float) $rows[2]['rolling_avg'], 0.01);
    }

    public function testWindowFunctionRangeUnbounded(): void
    {
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (3, '2024-03', 150)");

        // Cumulative sum using RANGE UNBOUNDED PRECEDING
        $stmt = $this->pdo->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative
            FROM sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(300.0, (float) $rows[1]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(450.0, (float) $rows[2]['cumulative'], 0.01);
    }

    public function testWindowFunctionLagLead(): void
    {
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (3, '2024-03', 150)");

        $stmt = $this->pdo->query("
            SELECT month, amount,
                   LAG(amount, 1) OVER (ORDER BY month) AS prev_amount,
                   LEAD(amount, 1) OVER (ORDER BY month) AS next_amount
            FROM sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['prev_amount']);
        $this->assertSame(200.0, (float) $rows[0]['next_amount']);
        $this->assertSame(100.0, (float) $rows[1]['prev_amount']);
        $this->assertSame(150.0, (float) $rows[1]['next_amount']);
        $this->assertNull($rows[2]['next_amount']);
    }

    public function testWindowFunctionRank(): void
    {
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (3, '2024-03', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (4, '2024-04', 300)");

        $stmt = $this->pdo->query("
            SELECT month, amount,
                   RANK() OVER (ORDER BY amount DESC) AS rnk,
                   DENSE_RANK() OVER (ORDER BY amount DESC) AS dense_rnk
            FROM sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        // 300 → rank 1, 200 → rank 2 (tie), 200 → rank 2, 100 → rank 4
        $this->assertSame(4, (int) $rows[0]['rnk']); // 100 → rank 4
        $this->assertSame(2, (int) $rows[1]['rnk']); // 200 → rank 2
        $this->assertSame(2, (int) $rows[2]['rnk']); // 200 → rank 2
        $this->assertSame(1, (int) $rows[3]['rnk']); // 300 → rank 1

        // Dense rank: 300→1, 200→2, 100→3 (no gap)
        $this->assertSame(3, (int) $rows[0]['dense_rnk']); // 100 → dense_rank 3
    }

    /**
     * INSERT ON CONFLICT DO NOTHING (standard syntax) on SQLite inserts BOTH rows.
     * This is a documented limitation — the shadow store doesn't enforce PK constraints.
     * Note: INSERT OR IGNORE (SQLite shorthand) correctly ignores the duplicate.
     */
    public function testInsertOnConflictDoNothingSqliteInsertsBoth(): void
    {
        $this->pdo->exec("INSERT INTO unique_items (id, name) VALUES (1, 'Widget')");
        $this->pdo->exec("INSERT INTO unique_items (id, name) VALUES (1, 'Widget Updated') ON CONFLICT(id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM unique_items WHERE id = 1');
        $count = (int) $stmt->fetch(PDO::FETCH_ASSOC)['c'];
        // Shadow store inserts both rows — DO NOTHING is not processed
        $this->assertSame(2, $count);
    }

    /**
     * INSERT OR IGNORE (SQLite-specific shorthand) correctly ignores duplicates.
     */
    public function testInsertOrIgnoreSqliteIgnoresDuplicate(): void
    {
        $this->pdo->exec("INSERT INTO unique_items (id, name) VALUES (1, 'Widget')");
        $this->pdo->exec("INSERT OR IGNORE INTO unique_items (id, name) VALUES (1, 'Widget Updated')");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM unique_items');
        $count = (int) $stmt->fetch(PDO::FETCH_ASSOC)['c'];
        // INSERT OR IGNORE only inserts 1 row
        $this->assertSame(1, $count);
    }

    public function testInsertOnConflictDoUpdateSqliteWorks(): void
    {
        $this->pdo->exec("INSERT INTO unique_items (id, name) VALUES (1, 'Widget')");
        $this->pdo->exec("INSERT INTO unique_items (id, name) VALUES (1, 'Widget Updated') ON CONFLICT(id) DO UPDATE SET name = excluded.name");

        $stmt = $this->pdo->query("SELECT name FROM unique_items WHERE id = 1");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // ON CONFLICT DO UPDATE works — the row is updated
        $this->assertSame('Widget Updated', $rows[0]['name']);
    }

    public function testWindowFunctionAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->pdo->exec("INSERT INTO sales (id, month, amount) VALUES (3, '2024-03', 150)");

        // Mutate
        $this->pdo->exec("UPDATE sales SET amount = 500 WHERE id = 2");
        $this->pdo->exec("DELETE FROM sales WHERE id = 3");

        $stmt = $this->pdo->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month) AS cumulative
            FROM sales
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(600.0, (float) $rows[1]['cumulative'], 0.01);
    }
}
