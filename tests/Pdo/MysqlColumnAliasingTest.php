<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests column aliasing patterns on MySQL PDO.
 * @spec pending
 */
class MysqlColumnAliasingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ca_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), qty INT, category VARCHAR(10))';
    }

    protected function getTableNames(): array
    {
        return ['ca_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (1, 'Widget', 10.50, 100, 'A')");
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (2, 'Gadget', 25.00, 50, 'A')");
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (3, 'Doohickey', 5.75, 200, 'B')");
    }

    public function testExpressionAlias(): void
    {
        $stmt = $this->pdo->query("SELECT name, price * qty AS total_value FROM ca_items ORDER BY total_value DESC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertEqualsWithDelta(1250.0, (float) $rows[0]['total_value'], 0.01);
    }

    public function testAggregateAlias(): void
    {
        $stmt = $this->pdo->query("
            SELECT category AS cat, COUNT(*) AS item_count, SUM(price * qty) AS total_value
            FROM ca_items GROUP BY category ORDER BY cat
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['cat']);
        $this->assertSame(2, (int) $rows[0]['item_count']);
    }

    public function testCaseExpressionAlias(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                   CASE WHEN price > 20 THEN 'expensive' WHEN price > 8 THEN 'moderate' ELSE 'cheap' END AS tier
            FROM ca_items ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('moderate', $rows[0]['tier']);
        $this->assertSame('expensive', $rows[1]['tier']);
        $this->assertSame('cheap', $rows[2]['tier']);
    }

    public function testCoalesceAlias(): void
    {
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (4, 'Unknown', 0, 0, NULL)");

        $stmt = $this->pdo->query("SELECT name, COALESCE(category, 'Uncategorized') AS display_cat FROM ca_items ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['display_cat']);
        $this->assertSame('Uncategorized', $rows[3]['display_cat']);
    }
}
