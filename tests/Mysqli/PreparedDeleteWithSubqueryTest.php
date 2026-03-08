<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared DELETE with subqueries via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedDeleteWithSubqueryTest (PDO).
 * @spec SPEC-4.3
 */
class PreparedDeleteWithSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pdel_customers (id INT PRIMARY KEY, name VARCHAR(50), tier VARCHAR(20))',
            'CREATE TABLE mi_pdel_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pdel_orders', 'mi_pdel_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (1, 'Alice', 'gold')");
        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (2, 'Bob', 'silver')");
        $this->mysqli->query("INSERT INTO mi_pdel_customers VALUES (3, 'Charlie', 'bronze')");
        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (1, 1, 100.00)');
        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (2, 1, 200.00)');
        $this->mysqli->query('INSERT INTO mi_pdel_orders VALUES (3, 2, 50.00)');
    }

    /**
     * Prepared DELETE with IN subquery.
     */
    public function testPreparedDeleteWithInSubquery(): void
    {
        $stmt = $this->mysqli->prepare('
            DELETE FROM mi_pdel_orders
            WHERE customer_id IN (SELECT id FROM mi_pdel_customers WHERE tier = ?)
        ');
        $tier = 'gold';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_orders');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Prepared DELETE with simple WHERE.
     */
    public function testPreparedDeleteWithSimpleWhere(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_customers WHERE tier = ?');
        $tier = 'bronze';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_customers');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Prepared DELETE then correlated SELECT.
     */
    public function testPreparedDeleteThenCorrelatedSelect(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_orders WHERE customer_id = ?');
        $custId = 1;
        $stmt->bind_param('i', $custId);
        $stmt->execute();

        $result = $this->mysqli->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM mi_pdel_orders o WHERE o.customer_id = c.id) AS cnt
            FROM mi_pdel_customers c
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']); // Alice — deleted
        $this->assertSame(1, (int) $rows[1]['cnt']); // Bob
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_pdel_customers WHERE tier = ?');
        $tier = 'gold';
        $stmt->bind_param('s', $tier);
        $stmt->execute();

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pdel_customers');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
