<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ZTD behavior with composite (multi-column) primary keys on MySQLi.
 * @spec pending
 */
class CompositePrimaryKeyTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cpk_order_items (order_id INT, item_id INT, product VARCHAR(255), quantity INT, price DECIMAL(10,2), PRIMARY KEY (order_id, item_id))',
            'CREATE TABLE mi_cpk_enrollments (student_id INT, course_id INT, semester VARCHAR(10), grade VARCHAR(5), PRIMARY KEY (student_id, course_id, semester))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cpk_order_items', 'mi_cpk_enrollments'];
    }


    private function seedOrderItems(): void
    {
        $this->mysqli->query("INSERT INTO mi_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->mysqli->query("INSERT INTO mi_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->mysqli->query("INSERT INTO mi_cpk_order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");
    }

    public function testInsertAndSelectWithCompositePk(): void
    {
        $this->seedOrderItems();

        $result = $this->mysqli->query('SELECT * FROM mi_cpk_order_items ORDER BY order_id, item_id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame('Gadget', $rows[1]['product']);
    }

    public function testUpdateWithCompositePk(): void
    {
        $this->seedOrderItems();

        $this->mysqli->query("UPDATE mi_cpk_order_items SET quantity = 10 WHERE order_id = 1 AND item_id = 1");

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_order_items WHERE order_id = 1 AND item_id = 1');
        $this->assertSame(10, (int) $result->fetch_assoc()['quantity']);

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_order_items WHERE order_id = 1 AND item_id = 2');
        $this->assertSame(1, (int) $result->fetch_assoc()['quantity']);
    }

    public function testDeleteWithCompositePk(): void
    {
        $this->seedOrderItems();

        $this->mysqli->query("DELETE FROM mi_cpk_order_items WHERE order_id = 1 AND item_id = 2");

        $result = $this->mysqli->query('SELECT COUNT(*) AS c FROM mi_cpk_order_items');
        $this->assertSame(2, (int) $result->fetch_assoc()['c']);
    }

    public function testUpdatePartialPkMatch(): void
    {
        $this->seedOrderItems();

        $this->mysqli->query("UPDATE mi_cpk_order_items SET price = 0.00 WHERE order_id = 1");

        $result = $this->mysqli->query('SELECT price FROM mi_cpk_order_items WHERE order_id = 1 ORDER BY item_id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0.0, (float) $rows[0]['price']);
        $this->assertSame(0.0, (float) $rows[1]['price']);

        $result = $this->mysqli->query('SELECT price FROM mi_cpk_order_items WHERE order_id = 2');
        $this->assertSame(9.99, (float) $result->fetch_assoc()['price']);
    }

    public function testThreeColumnCompositePk(): void
    {
        $this->mysqli->query("INSERT INTO mi_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024S', 'A')");
        $this->mysqli->query("INSERT INTO mi_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024F', 'B')");
        $this->mysqli->query("INSERT INTO mi_cpk_enrollments (student_id, course_id, semester, grade) VALUES (1, 102, '2024S', 'A')");
        $this->mysqli->query("INSERT INTO mi_cpk_enrollments (student_id, course_id, semester, grade) VALUES (2, 101, '2024S', 'C')");

        $this->mysqli->query("UPDATE mi_cpk_enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");

        $result = $this->mysqli->query("SELECT grade FROM mi_cpk_enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");
        $this->assertSame('A+', $result->fetch_assoc()['grade']);

        $result = $this->mysqli->query("SELECT grade FROM mi_cpk_enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024F'");
        $this->assertSame('B', $result->fetch_assoc()['grade']);
    }

    public function testPreparedStatementWithCompositePk(): void
    {
        $this->seedOrderItems();

        $stmt = $this->mysqli->prepare("UPDATE mi_cpk_order_items SET quantity = ? WHERE order_id = ? AND item_id = ?");
        $stmt->bind_param('iii', $qty, $orderId, $itemId);
        $qty = 20;
        $orderId = 1;
        $itemId = 1;
        $stmt->execute();

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_order_items WHERE order_id = 1 AND item_id = 1');
        $this->assertSame(20, (int) $result->fetch_assoc()['quantity']);

        $result = $this->mysqli->query('SELECT quantity FROM mi_cpk_order_items WHERE order_id = 1 AND item_id = 2');
        $this->assertSame(1, (int) $result->fetch_assoc()['quantity']);
    }

    public function testAggregationWithCompositePk(): void
    {
        $this->seedOrderItems();

        $result = $this->mysqli->query("
            SELECT order_id, COUNT(*) AS item_count, SUM(quantity * price) AS total
            FROM mi_cpk_order_items
            GROUP BY order_id
            ORDER BY order_id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['total'], 0.01);
    }
}
