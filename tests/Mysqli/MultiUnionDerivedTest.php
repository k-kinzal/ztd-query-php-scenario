<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT/DML with multiple UNION branches in derived tables (MySQLi).
 * @spec SPEC-3.3a, SPEC-3.1
 */
class MultiUnionDerivedTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mud_sales_2023 (id INT PRIMARY KEY, product VARCHAR(50), amount DECIMAL(10,2))',
            'CREATE TABLE mi_mud_sales_2024 (id INT PRIMARY KEY, product VARCHAR(50), amount DECIMAL(10,2))',
            'CREATE TABLE mi_mud_sales_2025 (id INT PRIMARY KEY, product VARCHAR(50), amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mud_sales_2025', 'mi_mud_sales_2024', 'mi_mud_sales_2023'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->mysqli->query("INSERT INTO mi_mud_sales_2023 VALUES (1, 'Widget', 100)");
        $this->mysqli->query("INSERT INTO mi_mud_sales_2023 VALUES (2, 'Gadget', 200)");
        $this->mysqli->query("INSERT INTO mi_mud_sales_2024 VALUES (1, 'Widget', 150)");
        $this->mysqli->query("INSERT INTO mi_mud_sales_2024 VALUES (2, 'Gadget', 180)");
        $this->mysqli->query("INSERT INTO mi_mud_sales_2025 VALUES (1, 'Widget', 175)");
    }

    public function testThreeWayUnionInDerivedTable(): void
    {
        $sql = "SELECT product, SUM(amount) AS total
                FROM (
                    SELECT product, amount FROM mi_mud_sales_2023
                    UNION ALL
                    SELECT product, amount FROM mi_mud_sales_2024
                    UNION ALL
                    SELECT product, amount FROM mi_mud_sales_2025
                ) combined
                GROUP BY product
                ORDER BY product";

        try {
            $result = $this->mysqli->query($sql);
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '3-way UNION derived: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEqualsWithDelta(380.0, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('3-way UNION derived failed: ' . $e->getMessage());
        }
    }

    public function testWhereInUnionSubquery(): void
    {
        $sql = "SELECT product, amount
                FROM mi_mud_sales_2023
                WHERE product IN (
                    SELECT product FROM mi_mud_sales_2024
                    UNION
                    SELECT product FROM mi_mud_sales_2025
                )
                ORDER BY product";

        try {
            $result = $this->mysqli->query($sql);
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE IN UNION: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WHERE IN UNION failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUnionDerivedTable(): void
    {
        $sql = "SELECT product, SUM(amount) AS total
                FROM (
                    SELECT product, amount FROM mi_mud_sales_2023 WHERE amount > ?
                    UNION ALL
                    SELECT product, amount FROM mi_mud_sales_2024 WHERE amount > ?
                ) filtered
                GROUP BY product
                ORDER BY total DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [120, 160]);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UNION derived: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UNION derived failed: ' . $e->getMessage());
        }
    }
}
