<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subqueries in various SQL positions (ORDER BY, HAVING, SELECT list,
 * CASE, nested WHERE) to verify CTE rewriting handles them correctly.
 * @spec pending
 */
class SqliteSubqueryPositionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sp_products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)',
            'CREATE TABLE sp_orders (id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sp_products', 'sp_orders'];
    }


    public function testScalarSubqueryInSelectList(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name,
                   (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) AS total_qty
            FROM sp_products p
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $rows[0]['total_qty']); // Widget: 3+2
        $this->assertSame(1, (int) $rows[1]['total_qty']); // Gadget: 1
        $this->assertSame(5, (int) $rows[2]['total_qty']); // Doohickey: 5
        $this->assertNull($rows[3]['total_qty']); // Thingamajig: no orders
    }

    public function testSubqueryInOrderBy(): void
    {
        // Order products by their total ordered quantity (descending)
        $stmt = $this->pdo->query("
            SELECT p.name
            FROM sp_products p
            ORDER BY (SELECT COALESCE(SUM(o.qty), 0) FROM sp_orders o WHERE o.product_id = p.id) DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Widget: 5, Doohickey: 5, Gadget: 1, Thingamajig: 0
        // Widget and Doohickey tie at 5 — order between them may vary
        $names = array_column($rows, 'name');
        $this->assertSame('Thingamajig', $names[3]); // Least ordered, last
        $this->assertSame('Gadget', $names[2]); // 1 order, second to last
    }

    public function testSubqueryInHaving(): void
    {
        // Find categories where total ordered qty exceeds average category qty
        $stmt = $this->pdo->query("
            SELECT p.category, SUM(o.qty) AS cat_qty
            FROM sp_products p
            JOIN sp_orders o ON o.product_id = p.id
            GROUP BY p.category
            HAVING SUM(o.qty) > (
                SELECT AVG(sub.cat_total) FROM (
                    SELECT SUM(o2.qty) AS cat_total
                    FROM sp_products p2
                    JOIN sp_orders o2 ON o2.product_id = p2.id
                    GROUP BY p2.category
                ) sub
            )
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // A: Widget(5)+Gadget(1)=6, B: Doohickey(5)=5, avg=5.5
        // Only A (6) exceeds 5.5
        $this->assertCount(1, $rows);
        $this->assertSame('A', $rows[0]['category']);
    }

    public function testNestedWhereWithMultipleOperators(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM sp_products p
            WHERE (p.price > 8 AND p.category = 'A')
               OR (p.price < 10 AND p.id IN (SELECT o.product_id FROM sp_orders o WHERE o.qty >= 5))
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Condition 1: price>8 AND cat=A → Widget(10), Gadget(25)
        // Condition 2: price<10 AND id in orders with qty>=5 → Doohickey(5, qty=5)
        $names = array_column($rows, 'name');
        $this->assertSame(['Widget', 'Gadget', 'Doohickey'], $names);
    }

    public function testCaseWithSubqueryInSelectList(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name,
                   CASE
                       WHEN (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) > 3 THEN 'high'
                       WHEN (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) > 0 THEN 'low'
                       ELSE 'none'
                   END AS demand
            FROM sp_products p
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('high', $rows[0]['demand']);  // Widget: 5
        $this->assertSame('low', $rows[1]['demand']);   // Gadget: 1
        $this->assertSame('high', $rows[2]['demand']);  // Doohickey: 5
        $this->assertSame('none', $rows[3]['demand']);  // Thingamajig: 0
    }

    public function testSubqueryReflectsMutations(): void
    {
        // Delete some orders
        $this->pdo->exec("DELETE FROM sp_orders WHERE product_id = 1");

        // Scalar subquery should reflect deletion
        $stmt = $this->pdo->query("
            SELECT p.name,
                   (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) AS total_qty
            FROM sp_products p
            WHERE p.id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['total_qty']); // No orders left for Widget
    }

    public function testExistsNotExistsCombined(): void
    {
        // Products that have orders AND don't have orders with qty > 4
        $stmt = $this->pdo->query("
            SELECT p.name FROM sp_products p
            WHERE EXISTS (SELECT 1 FROM sp_orders o WHERE o.product_id = p.id)
              AND NOT EXISTS (SELECT 1 FROM sp_orders o WHERE o.product_id = p.id AND o.qty > 4)
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Widget: has orders, none >4 (3 and 2) → yes
        // Gadget: has orders, none >4 (1) → yes
        // Doohickey: has orders, one with qty=5 >4 → no
        $names = array_column($rows, 'name');
        $this->assertSame(['Widget', 'Gadget'], $names);
    }

    public function testMultipleSubqueriesInWhere(): void
    {
        // Products with price above average AND total orders below max product orders
        $stmt = $this->pdo->query("
            SELECT p.name FROM sp_products p
            WHERE p.price > (SELECT AVG(p2.price) FROM sp_products p2)
              AND (SELECT COALESCE(SUM(o.qty), 0) FROM sp_orders o WHERE o.product_id = p.id) <
                  (SELECT MAX(sub.total) FROM (
                      SELECT SUM(o2.qty) AS total FROM sp_orders o2 GROUP BY o2.product_id
                  ) sub)
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // avg price = (10+25+5+50)/4 = 22.5
        // Above avg: Gadget(25), Thingamajig(50)
        // Max orders by product: Widget(5) or Doohickey(5) → max=5
        // Gadget orders: 1 < 5 → yes
        // Thingamajig orders: 0 < 5 → yes
        $names = array_column($rows, 'name');
        $this->assertSame(['Gadget', 'Thingamajig'], $names);
    }
}
