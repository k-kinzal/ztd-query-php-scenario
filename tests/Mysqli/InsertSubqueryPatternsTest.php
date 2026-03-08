<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT ... SELECT subquery patterns on MySQLi.
 * Like MySQL PDO, computed columns and GROUP BY aggregation work correctly.
 * @spec SPEC-4.1a
 */
class InsertSubqueryPatternsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_isp_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))',
            'CREATE TABLE mi_isp_archive (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))',
            'CREATE TABLE mi_isp_stats (category VARCHAR(30) PRIMARY KEY, product_count INT, avg_price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_isp_stats', 'mi_isp_archive', 'mi_isp_products'];
    }


    public function testInsertSelectWithWhereFilter(): void
    {
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $this->mysqli->query("INSERT INTO mi_isp_archive (id, name, price, category) SELECT id, name, price, category FROM mi_isp_products WHERE category = 'electronics'");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_isp_archive");
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    public function testInsertSelectWithComputedColumnsWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");

        $this->mysqli->query("INSERT INTO mi_isp_archive (id, name, price, category) SELECT id, name, price * 2, category FROM mi_isp_products");

        $result = $this->mysqli->query("SELECT price FROM mi_isp_archive WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(59.98, (float) $row['price'], 0.01);
    }

    public function testInsertSelectWithGroupByAggregationWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->mysqli->query("INSERT INTO mi_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $this->mysqli->query("
            INSERT INTO mi_isp_stats (category, product_count, avg_price)
            SELECT category, COUNT(*), AVG(price)
            FROM mi_isp_products
            GROUP BY category
        ");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query("SELECT * FROM mi_isp_stats ORDER BY category");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['product_count']);
    }
}
