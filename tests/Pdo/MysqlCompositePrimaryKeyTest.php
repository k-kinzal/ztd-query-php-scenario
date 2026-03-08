<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ZTD behavior with composite (multi-column) primary keys on MySQL PDO.
 * @spec pending
 */
class MysqlCompositePrimaryKeyTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_cpk_order_items (order_id INT, item_id INT, product VARCHAR(255), quantity INT, price DECIMAL(10,2), PRIMARY KEY (order_id, item_id))',
            'CREATE TABLE mysql_cpk_enrollments (student_id INT, course_id INT, semester VARCHAR(10), grade VARCHAR(5), PRIMARY KEY (student_id, course_id, semester))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_cpk_order_items', 'mysql_cpk_enrollments'];
    }


    private function seedOrderItems(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO mysql_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO mysql_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");
    }

    public function testInsertAndSelectWithCompositePk(): void
    {
        $this->seedOrderItems();

        $stmt = $this->pdo->query('SELECT * FROM mysql_cpk_order_items ORDER BY order_id, item_id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame('Gadget', $rows[1]['product']);
    }

    public function testUpdateWithCompositePk(): void
    {
        $this->seedOrderItems();

        $this->pdo->exec("UPDATE mysql_cpk_order_items SET quantity = 10 WHERE order_id = 1 AND item_id = 1");

        $stmt = $this->pdo->query('SELECT quantity FROM mysql_cpk_order_items WHERE order_id = 1 AND item_id = 1');
        $this->assertSame(10, (int) $stmt->fetch(PDO::FETCH_ASSOC)['quantity']);

        $stmt = $this->pdo->query('SELECT quantity FROM mysql_cpk_order_items WHERE order_id = 1 AND item_id = 2');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['quantity']);
    }

    public function testDeleteWithCompositePk(): void
    {
        $this->seedOrderItems();

        $this->pdo->exec("DELETE FROM mysql_cpk_order_items WHERE order_id = 1 AND item_id = 2");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM mysql_cpk_order_items');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testUpdatePartialPkMatch(): void
    {
        $this->seedOrderItems();

        $this->pdo->exec("UPDATE mysql_cpk_order_items SET price = 0.00 WHERE order_id = 1");

        $stmt = $this->pdo->query('SELECT price FROM mysql_cpk_order_items WHERE order_id = 1 ORDER BY item_id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0.0, (float) $rows[0]['price']);
        $this->assertSame(0.0, (float) $rows[1]['price']);

        $stmt = $this->pdo->query('SELECT price FROM mysql_cpk_order_items WHERE order_id = 2');
        $this->assertSame(9.99, (float) $stmt->fetch(PDO::FETCH_ASSOC)['price']);
    }

    public function testThreeColumnCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO mysql_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024F', 'B')");
        $this->pdo->exec("INSERT INTO mysql_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 102, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO mysql_cpk_enrollments (student_id, course_id, semester, grade) VALUES (2, 101, '2024S', 'C')");

        $this->pdo->exec("UPDATE mysql_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");

        $stmt = $this->pdo->query("SELECT grade FROM mysql_cpk_enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");
        $this->assertSame('A+', $stmt->fetch(PDO::FETCH_ASSOC)['grade']);

        $stmt = $this->pdo->query("SELECT grade FROM mysql_cpk_enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024F'");
        $this->assertSame('B', $stmt->fetch(PDO::FETCH_ASSOC)['grade']);
    }

    public function testPreparedStatementWithCompositePk(): void
    {
        $this->seedOrderItems();

        $stmt = $this->pdo->prepare("UPDATE mysql_cpk_order_items SET quantity = ? WHERE order_id = ? AND item_id = ?");
        $stmt->execute([20, 1, 1]);

        $stmt2 = $this->pdo->prepare("SELECT quantity FROM mysql_cpk_order_items WHERE order_id = ? AND item_id = ?");
        $stmt2->execute([1, 1]);
        $this->assertSame(20, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['quantity']);

        $stmt2->execute([1, 2]);
        $this->assertSame(1, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['quantity']);
    }

    public function testAggregationWithCompositePk(): void
    {
        $this->seedOrderItems();

        $stmt = $this->pdo->query("
            SELECT order_id, COUNT(*) AS item_count, SUM(quantity * price) AS total
            FROM mysql_cpk_order_items
            GROUP BY order_id
            ORDER BY order_id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['total'], 0.01);
    }

    public function testNoPhysicalDataLeak(): void
    {
        $this->seedOrderItems();

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM mysql_cpk_order_items');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
