<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZTD behavior with composite (multi-column) primary keys on SQLite.
 * UPDATE and DELETE depend on PK reflection, so composite PKs are a critical pattern.
 * @spec SPEC-3.6
 */
class SqliteCompositePrimaryKeyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, product TEXT, quantity INTEGER, price REAL, PRIMARY KEY (order_id, item_id))',
            'CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER, semester TEXT, grade TEXT, PRIMARY KEY (student_id, course_id, semester))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['order_items', 'enrollments'];
    }


    public function testInsertAndSelectWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        $stmt = $this->pdo->query('SELECT * FROM order_items ORDER BY order_id, item_id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame('Gadget', $rows[1]['product']);
    }

    public function testUpdateWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        // Update specific row by composite PK
        $this->pdo->exec("UPDATE order_items SET quantity = 10 WHERE order_id = 1 AND item_id = 1");

        $stmt = $this->pdo->query('SELECT quantity FROM order_items WHERE order_id = 1 AND item_id = 1');
        $this->assertSame(10, (int) $stmt->fetch(PDO::FETCH_ASSOC)['quantity']);

        // Other rows unchanged
        $stmt = $this->pdo->query('SELECT quantity FROM order_items WHERE order_id = 1 AND item_id = 2');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['quantity']);

        $stmt = $this->pdo->query('SELECT quantity FROM order_items WHERE order_id = 2 AND item_id = 1');
        $this->assertSame(5, (int) $stmt->fetch(PDO::FETCH_ASSOC)['quantity']);
    }

    public function testDeleteWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        $this->pdo->exec("DELETE FROM order_items WHERE order_id = 1 AND item_id = 2");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM order_items');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testUpdatePartialPkMatch(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        // Update all items for order_id = 1 (partial PK match)
        $this->pdo->exec("UPDATE order_items SET price = 0.00 WHERE order_id = 1");

        $stmt = $this->pdo->query('SELECT price FROM order_items WHERE order_id = 1 ORDER BY item_id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0.0, (float) $rows[0]['price']);
        $this->assertSame(0.0, (float) $rows[1]['price']);

        // order_id=2 unchanged
        $stmt = $this->pdo->query('SELECT price FROM order_items WHERE order_id = 2');
        $this->assertSame(9.99, (float) $stmt->fetch(PDO::FETCH_ASSOC)['price']);
    }

    public function testThreeColumnCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024F', 'B')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 102, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (2, 101, '2024S', 'C')");

        // Update specific enrollment
        $this->pdo->exec("UPDATE enrollments SET grade = 'A+' WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");

        $stmt = $this->pdo->query("SELECT grade FROM enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024S'");
        $this->assertSame('A+', $stmt->fetch(PDO::FETCH_ASSOC)['grade']);

        // Other enrollments unchanged
        $stmt = $this->pdo->query("SELECT grade FROM enrollments WHERE student_id = 1 AND course_id = 101 AND semester = '2024F'");
        $this->assertSame('B', $stmt->fetch(PDO::FETCH_ASSOC)['grade']);
    }

    public function testDeletePartialPkMatch(): void
    {
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 101, '2024F', 'B')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (1, 102, '2024S', 'A')");
        $this->pdo->exec("INSERT INTO enrollments (student_id, course_id, semester, grade) VALUES (2, 101, '2024S', 'C')");

        // Delete all enrollments for student 1 in course 101
        $this->pdo->exec("DELETE FROM enrollments WHERE student_id = 1 AND course_id = 101");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS c FROM enrollments');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT * FROM enrollments ORDER BY student_id, course_id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(102, (int) $rows[0]['course_id']);
        $this->assertSame(2, (int) $rows[1]['student_id']);
    }

    public function testJoinWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        $stmt = $this->pdo->query("
            SELECT a.product, a.quantity AS q1, b.quantity AS q2
            FROM order_items a
            JOIN order_items b ON a.product = b.product AND a.order_id < b.order_id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
    }

    public function testPreparedStatementWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");

        $stmt = $this->pdo->prepare("UPDATE order_items SET quantity = ? WHERE order_id = ? AND item_id = ?");
        $stmt->execute([20, 1, 1]);

        $stmt2 = $this->pdo->prepare("SELECT quantity FROM order_items WHERE order_id = ? AND item_id = ?");
        $stmt2->execute([1, 1]);
        $this->assertSame(20, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['quantity']);

        $stmt2->execute([1, 2]);
        $this->assertSame(1, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['quantity']);
    }

    public function testAggregationWithCompositePk(): void
    {
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (order_id, item_id, product, quantity, price) VALUES (2, 1, 'Widget', 5, 9.99)");

        $stmt = $this->pdo->query("
            SELECT order_id, COUNT(*) AS item_count, SUM(quantity * price) AS total
            FROM order_items
            GROUP BY order_id
            ORDER BY order_id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['total'], 0.01);
    }
}
