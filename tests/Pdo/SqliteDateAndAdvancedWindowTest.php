<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests date/time functions and advanced window functions on SQLite.
 * @spec pending
 */
class SqliteDateAndAdvancedWindowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE events (id INTEGER PRIMARY KEY, title TEXT, event_date TEXT, category TEXT, amount REAL)',
            'CREATE TABLE nat_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)',
            'CREATE TABLE nat_customers (id INTEGER PRIMARY KEY, name TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['events', 'nat_orders', 'nat_customers'];
    }


    // --- Date Functions ---

    public function testDateFunction(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'Meeting', '2024-03-15', 'work', 100)");

        $stmt = $this->pdo->query("SELECT DATE(event_date) AS d FROM events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-03-15', $row['d']);
    }

    public function testDateArithmetic(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'Meeting', '2024-03-15', 'work', 100)");

        $stmt = $this->pdo->query("SELECT DATE(event_date, '+7 days') AS next_week FROM events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-03-22', $row['next_week']);
    }

    public function testStrftime(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'Meeting', '2024-03-15', 'work', 100)");

        $stmt = $this->pdo->query("SELECT strftime('%Y', event_date) AS yr, strftime('%m', event_date) AS mo FROM events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024', $row['yr']);
        $this->assertSame('03', $row['mo']);
    }

    public function testDateComparisonInWhere(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'Old', '2023-01-01', 'work', 50)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'Recent', '2024-06-15', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'Future', '2025-01-01', 'work', 200)");

        $stmt = $this->pdo->query("SELECT title FROM events WHERE event_date >= '2024-01-01' ORDER BY event_date");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Recent', $rows[0]['title']);
        $this->assertSame('Future', $rows[1]['title']);
    }

    public function testGroupByDatePart(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-10', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-20', 'work', 200)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-02-05', 'work', 150)");

        $stmt = $this->pdo->query("
            SELECT strftime('%Y-%m', event_date) AS month, SUM(amount) AS total
            FROM events
            GROUP BY strftime('%Y-%m', event_date)
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertSame('2024-02', $rows[1]['month']);
    }

    // --- Advanced Window Functions ---

    public function testNtile(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-01', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-02', 'work', 200)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-01-03', 'work', 300)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (4, 'D', '2024-01-04', 'work', 400)");

        $stmt = $this->pdo->query("
            SELECT title, amount, NTILE(2) OVER (ORDER BY amount) AS tile
            FROM events
            ORDER BY amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame(1, (int) $rows[0]['tile']); // 100 → tile 1
        $this->assertSame(1, (int) $rows[1]['tile']); // 200 → tile 1
        $this->assertSame(2, (int) $rows[2]['tile']); // 300 → tile 2
        $this->assertSame(2, (int) $rows[3]['tile']); // 400 → tile 2
    }

    public function testFirstValueLastValue(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-01', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-02', 'work', 200)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-01-03', 'personal', 300)");

        $stmt = $this->pdo->query("
            SELECT title, amount,
                   FIRST_VALUE(amount) OVER (ORDER BY amount) AS first_amt,
                   LAST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
            FROM events
            ORDER BY amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['first_amt'], 0.01);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['last_amt'], 0.01);
    }

    public function testPartitionByWithWindowFunction(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-01', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-02', 'work', 200)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-01-03', 'personal', 300)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (4, 'D', '2024-01-04', 'personal', 400)");

        $stmt = $this->pdo->query("
            SELECT title, category, amount,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS rn,
                   SUM(amount) OVER (PARTITION BY category) AS cat_total
            FROM events
            ORDER BY category, amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);

        // personal: C=300 (rn=1), D=400 (rn=2), total=700
        $this->assertSame('personal', $rows[0]['category']);
        $this->assertSame(1, (int) $rows[0]['rn']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['cat_total'], 0.01);

        // work: A=100 (rn=1), B=200 (rn=2), total=300
        $this->assertSame('work', $rows[2]['category']);
        $this->assertSame(1, (int) $rows[2]['rn']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[2]['cat_total'], 0.01);
    }

    public function testWindowFunctionWithPartitionAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-01', 'work', 100)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-02', 'work', 200)");
        $this->pdo->exec("INSERT INTO events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-01-03', 'personal', 300)");

        // Mutate: move B to personal and update amount
        $this->pdo->exec("UPDATE events SET category = 'personal', amount = 250 WHERE id = 2");

        $stmt = $this->pdo->query("
            SELECT category, SUM(amount) OVER (PARTITION BY category) AS cat_total
            FROM events
            ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);

        // personal: B=250, C=300, total=550
        $personalRows = array_filter($rows, fn($r) => $r['category'] === 'personal');
        $this->assertEqualsWithDelta(550.0, (float) array_values($personalRows)[0]['cat_total'], 0.01);

        // work: A=100, total=100
        $workRows = array_filter($rows, fn($r) => $r['category'] === 'work');
        $this->assertEqualsWithDelta(100.0, (float) array_values($workRows)[0]['cat_total'], 0.01);
    }

    // --- NATURAL JOIN ---

    public function testNaturalJoin(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE nat_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)');
        $raw->exec('CREATE TABLE nat_customers (id INTEGER PRIMARY KEY, name TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO nat_customers (id, name) VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO nat_customers (id, name) VALUES (2, 'Bob')");

        $pdo->exec("INSERT INTO nat_orders (id, customer_id, total) VALUES (1, 1, 100)");
        $pdo->exec("INSERT INTO nat_orders (id, customer_id, total) VALUES (2, 1, 200)");

        // NATURAL JOIN matches on `id` column (shared between both tables)
        $stmt = $pdo->query("SELECT nat_orders.id, nat_orders.total, nat_customers.name FROM nat_orders NATURAL JOIN nat_customers");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // NATURAL JOIN matches on `id` — order id 1 matches customer id 1, order id 2 matches customer id 2
        $this->assertCount(2, $rows);
    }
}
