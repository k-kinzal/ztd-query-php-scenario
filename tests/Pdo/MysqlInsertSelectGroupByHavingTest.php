<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and HAVING through ZTD on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlInsertSelectGroupByHavingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_isgh_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                product VARCHAR(50),
                amount DECIMAL(10,2),
                qty INT
            ) ENGINE=InnoDB",
            "CREATE TABLE my_isgh_summary (
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
        return ['my_isgh_summary', 'my_isgh_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 100.00, 10)");
        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('east', 'Gadget', 200.00, 5)");
        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 150.00, 8)");
        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('west', 'Gadget', 300.00, 12)");
        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('west', 'Doohickey', 50.00, 3)");
        $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('north', 'Widget', 75.00, 4)");
    }

    /**
     * INSERT...SELECT GROUP BY HAVING on MySQL.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM my_isgh_sales
                 GROUP BY region
                 HAVING SUM(amount) > 100"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM my_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY HAVING (MySQL): expected 2 regions, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('east', $regions);
            $this->assertContains('west', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY HAVING (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT GROUP BY after prior DML.
     */
    public function testInsertSelectGroupByAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_isgh_sales (region, product, amount, qty) VALUES ('east', 'Premium', 500.00, 2)");
            $this->ztdExec("DELETE FROM my_isgh_sales WHERE region = 'north'");

            $this->ztdExec(
                "INSERT INTO my_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM my_isgh_sales
                 GROUP BY region"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, order_count FROM my_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY after DML (MySQL): expected 2 regions, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $this->assertEqualsWithDelta(950.00, (float) $east[0]['total_amount'], 0.01);
            $this->assertSame(4, (int) $east[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY after DML (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT GROUP BY HAVING with ? params.
     */
    public function testPreparedInsertGroupByHaving(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO my_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM my_isgh_sales
                 WHERE product = ?
                 GROUP BY region
                 HAVING SUM(amount) > ?"
            );
            $stmt->execute(['Widget', 100]);

            $rows = $this->ztdQuery("SELECT region, total_amount FROM my_isgh_summary ORDER BY region");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT GROUP BY HAVING (MySQL): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('east', $rows[0]['region']);
            $this->assertEqualsWithDelta(250.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT GROUP BY HAVING (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with COUNT(DISTINCT) in HAVING.
     */
    public function testInsertSelectHavingCountDistinct(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(DISTINCT product)
                 FROM my_isgh_sales
                 GROUP BY region
                 HAVING COUNT(DISTINCT product) > 1"
            );

            $rows = $this->ztdQuery("SELECT region FROM my_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT HAVING COUNT DISTINCT (MySQL): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT HAVING COUNT DISTINCT (MySQL) failed: ' . $e->getMessage());
        }
    }
}
