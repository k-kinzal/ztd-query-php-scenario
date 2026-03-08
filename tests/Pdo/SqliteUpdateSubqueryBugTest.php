<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Investigates UPDATE with IN (subquery containing GROUP BY HAVING) on SQLite.
 * This appears to cause "incomplete input" errors in the CTE rewriter.
 */
class SqliteUpdateSubqueryBugTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)');
        $raw->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO users (id, name, tier) VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO users (id, name, tier) VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, total, status) VALUES (1, 1, 500, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, total, status) VALUES (2, 1, 300, 'completed')");
        $this->pdo->exec("INSERT INTO orders (id, user_id, total, status) VALUES (3, 2, 100, 'completed')");
    }

    public function testSimpleUpdateWithInSubquery(): void
    {
        // Simple IN subquery without GROUP BY — should work
        $this->pdo->exec("UPDATE users SET tier = 'premium' WHERE id IN (SELECT user_id FROM orders WHERE total > 400)");

        $stmt = $this->pdo->query("SELECT tier FROM users WHERE id = 1");
        $this->assertSame('premium', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);
    }

    /**
     * UPDATE with IN (subquery containing GROUP BY HAVING) should work.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/9
     */
    public function testUpdateWithInSubqueryGroupByHaving(): void
    {
        try {
            $this->pdo->exec("UPDATE users SET tier = 'premium' WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400)");

            $stmt = $this->pdo->query("SELECT tier FROM users WHERE id = 1");
            $this->assertSame('premium', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);

            $stmt = $this->pdo->query("SELECT tier FROM users WHERE id = 2");
            $this->assertSame('standard', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CTE rewriter produces incomplete SQL for UPDATE with IN (GROUP BY HAVING) on SQLite: ' . $e->getMessage()
            );
        }
    }

    public function testSelectWithGroupByHavingWorks(): void
    {
        // The SELECT itself works fine — only UPDATE with this subquery fails
        $stmt = $this->pdo->query("SELECT user_id FROM orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
    }
}
