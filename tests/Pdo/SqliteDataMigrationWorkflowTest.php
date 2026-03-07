<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests a data migration workflow: create archive tables, migrate data with transformations,
 * verify data integrity with complex queries. Exercises many SQL patterns together.
 */
class SqliteDataMigrationWorkflowTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, tier TEXT, created_date TEXT)');
        $raw->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT, amount REAL, status TEXT, created_date TEXT)');
        $raw->exec('CREATE TABLE user_stats (user_id INTEGER PRIMARY KEY, total_orders INTEGER, total_spent REAL, last_order_date TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * INSERT...SELECT with LEFT JOIN + GROUP BY + aggregation inserts rows
     * but all column values are NULL — the shadow store doesn't transfer
     * SELECT result values to the inserted rows.
     */
    public function testInsertSelectWithJoinGroupByProducesNullValues(): void
    {
        $this->pdo->exec("INSERT INTO users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $affected = $this->pdo->exec("
            INSERT INTO user_stats (user_id, total_orders, total_spent, last_order_date)
            SELECT u.id, COUNT(o.id), COALESCE(SUM(o.amount), 0), MAX(o.created_date)
            FROM users u
            LEFT JOIN orders o ON o.user_id = u.id
            GROUP BY u.id
        ");
        $this->assertSame(2, $affected); // 2 rows inserted

        // But all values are NULL — INSERT...SELECT with JOIN+GROUP BY doesn't transfer values
        $stmt = $this->pdo->query("SELECT * FROM user_stats ORDER BY user_id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertNull($rows[0]['user_id']); // Expected: 1, Actual: NULL
        $this->assertNull($rows[0]['total_orders']); // Expected: 2, Actual: NULL
    }

    public function testMigrationWorkflowWithManualInserts(): void
    {
        // Workaround: compute stats via SELECT, then INSERT manually
        $this->pdo->exec("INSERT INTO users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");
        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 2, 'Widget', 75, 'completed', '2024-01-15')");

        // Compute stats via SELECT
        $stmt = $this->pdo->query("
            SELECT u.id, COUNT(o.id) AS total_orders, SUM(o.amount) AS total_spent, MAX(o.created_date) AS last_order
            FROM users u LEFT JOIN orders o ON o.user_id = u.id
            GROUP BY u.id ORDER BY u.id
        ");
        $stats = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $stats);

        // Insert stats manually
        foreach ($stats as $s) {
            $this->pdo->exec(sprintf(
                "INSERT INTO user_stats VALUES (%d, %d, %s, '%s')",
                (int) $s['id'], (int) $s['total_orders'], (float) $s['total_spent'], $s['last_order']
            ));
        }

        // Verify
        $check = $this->pdo->query("SELECT * FROM user_stats ORDER BY user_id");
        $rows = $check->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
        $this->assertSame(2, (int) $rows[0]['total_orders']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);

        // Update tiers based on stats
        $this->pdo->exec("UPDATE users SET tier = 'platinum' WHERE id IN (SELECT user_id FROM user_stats WHERE total_spent >= 200)");

        $stmt = $this->pdo->query("SELECT name, tier FROM users WHERE tier = 'platinum'");
        $platinum = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $platinum);
        $this->assertSame('Alice', $platinum[0]['name']);

        // Verify shadow isolation
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testSubqueryInSelectCase(): void
    {
        $this->pdo->exec("INSERT INTO users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");

        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 1, 'Gadget', 200, 'completed', '2024-02-10')");

        $stmt = $this->pdo->query("
            SELECT u.name,
                   CASE
                       WHEN (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) > 1 THEN 'active'
                       WHEN (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) = 1 THEN 'one-time'
                       ELSE 'inactive'
                   END AS activity_level
            FROM users u
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['activity_level']); // Alice has 2 orders
        $this->assertSame('inactive', $rows[1]['activity_level']); // Bob has 0
    }

    public function testComplexWhereWithMultipleSubqueries(): void
    {
        $this->pdo->exec("INSERT INTO users VALUES (1, 'Alice', 'alice@co.com', 'gold', '2023-01-15')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'Bob', 'bob@co.com', 'silver', '2023-06-20')");
        $this->pdo->exec("INSERT INTO users VALUES (3, 'Charlie', 'charlie@co.com', 'bronze', '2024-01-01')");

        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, 'Widget', 100, 'completed', '2024-01-05')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 2, 'Gadget', 200, 'completed', '2024-02-10')");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 3, 'Widget', 50, 'completed', '2024-03-01')");

        // Users who spent above average AND ordered after a specific date
        $stmt = $this->pdo->query("
            SELECT u.name
            FROM users u
            WHERE u.id IN (
                SELECT o.user_id FROM orders o
                WHERE o.status = 'completed'
                GROUP BY o.user_id
                HAVING SUM(o.amount) > (SELECT AVG(amount) FROM orders WHERE status = 'completed')
            )
            AND u.created_date < '2024-01-01'
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Average = (100+200+50)/3 ≈ 116.67
        // Alice: 100 < 116.67 → no, Bob: 200 > 116.67 → yes, Charlie: 50 < 116.67 AND created_date NOT < 2024-01-01
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }
}
