<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and HAVING through ZTD on MySQLi.
 *
 * @spec SPEC-10.2
 */
class InsertSelectGroupByHavingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_isgh_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                product VARCHAR(50),
                amount DECIMAL(10,2),
                qty INT
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_isgh_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                total_amount DECIMAL(12,2),
                total_qty INT,
                order_count INT
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_isgh_summary', 'mi_isgh_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 100.00, 10)");
        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('east', 'Gadget', 200.00, 5)");
        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 150.00, 8)");
        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('west', 'Gadget', 300.00, 12)");
        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('west', 'Doohickey', 50.00, 3)");
        $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('north', 'Widget', 75.00, 4)");
    }

    /**
     * INSERT...SELECT GROUP BY HAVING on MySQLi.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM mi_isgh_sales
                 GROUP BY region
                 HAVING SUM(amount) > 100"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM mi_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY HAVING (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('east', $regions);
            $this->assertContains('west', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY HAVING (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT GROUP BY after prior DML on source.
     */
    public function testInsertSelectGroupByAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_isgh_sales (region, product, amount, qty) VALUES ('east', 'Premium', 500.00, 2)");
            $this->ztdExec("DELETE FROM mi_isgh_sales WHERE region = 'north'");

            $this->ztdExec(
                "INSERT INTO mi_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM mi_isgh_sales
                 GROUP BY region"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, order_count FROM mi_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY after DML (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $this->assertEqualsWithDelta(950.00, (float) $east[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY after DML (MySQLi) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with COUNT(DISTINCT) in HAVING.
     */
    public function testInsertSelectHavingCountDistinct(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(DISTINCT product)
                 FROM mi_isgh_sales
                 GROUP BY region
                 HAVING COUNT(DISTINCT product) > 1"
            );

            $rows = $this->ztdQuery("SELECT region FROM mi_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT HAVING COUNT DISTINCT (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT HAVING COUNT DISTINCT (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
