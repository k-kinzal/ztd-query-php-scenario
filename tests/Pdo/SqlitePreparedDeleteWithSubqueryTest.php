<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared DELETE with subqueries on SQLite.
 *
 * DELETE with EXISTS/IN subqueries via prepared statements ensures
 * parameter binding works correctly with the CTE rewriter.
 */
class SqlitePreparedDeleteWithSubqueryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_pdel_customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)');
        $raw->exec('CREATE TABLE sl_pdel_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sl_pdel_customers VALUES (1, 'Alice', 'gold')");
        $this->pdo->exec("INSERT INTO sl_pdel_customers VALUES (2, 'Bob', 'silver')");
        $this->pdo->exec("INSERT INTO sl_pdel_customers VALUES (3, 'Charlie', 'bronze')");

        $this->pdo->exec("INSERT INTO sl_pdel_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO sl_pdel_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO sl_pdel_orders VALUES (3, 2, 50.00)");
    }

    /**
     * Prepared DELETE with IN subquery and parameter.
     */
    public function testPreparedDeleteWithInSubquery(): void
    {
        $stmt = $this->pdo->prepare('
            DELETE FROM sl_pdel_orders
            WHERE customer_id IN (SELECT id FROM sl_pdel_customers WHERE tier = ?)
        ');
        $stmt->execute(['gold']);

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_pdel_orders');
        $this->assertSame(1, (int) $qstmt->fetchColumn()); // Only Bob's order remains
    }

    /**
     * Prepared DELETE with scalar subquery comparison and parameter.
     */
    public function testPreparedDeleteWithScalarSubquery(): void
    {
        $stmt = $this->pdo->prepare('
            DELETE FROM sl_pdel_orders
            WHERE amount > (SELECT AVG(amount) FROM sl_pdel_orders WHERE customer_id = ?)
        ');
        $stmt->execute([1]); // AVG for Alice = (100+200)/2 = 150, delete amount > 150

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_pdel_orders');
        $this->assertSame(2, (int) $qstmt->fetchColumn()); // 100 and 50 remain
    }

    /**
     * Prepared DELETE with simple WHERE parameter.
     */
    public function testPreparedDeleteWithSimpleWhere(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM sl_pdel_customers WHERE tier = ?');
        $stmt->execute(['bronze']);

        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_pdel_customers');
        $this->assertSame(2, (int) $qstmt->fetchColumn());
    }

    /**
     * Prepared DELETE then verify correlated SELECT consistency.
     */
    public function testPreparedDeleteThenCorrelatedSelect(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM sl_pdel_orders WHERE customer_id = ?');
        $stmt->execute([1]);

        $qstmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM sl_pdel_orders o WHERE o.customer_id = c.id) AS cnt
            FROM sl_pdel_customers c
            ORDER BY c.id
        ');
        $rows = $qstmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']); // Alice — deleted
        $this->assertSame(1, (int) $rows[1]['cnt']); // Bob
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM sl_pdel_customers WHERE tier = ?');
        $stmt->execute(['gold']);

        $this->pdo->disableZtd();
        $qstmt = $this->pdo->query('SELECT COUNT(*) FROM sl_pdel_customers');
        $this->assertSame(0, (int) $qstmt->fetchColumn());
    }
}
