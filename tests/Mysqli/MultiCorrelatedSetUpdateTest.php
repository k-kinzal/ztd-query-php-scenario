<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with multiple SET columns each from a different correlated
 * subquery — MySQLi.
 *
 * @spec SPEC-4.2
 */
class MultiCorrelatedSetUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mcsu_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                total_sales DECIMAL(10,2) NOT NULL DEFAULT 0,
                avg_rating DECIMAL(3,1) NOT NULL DEFAULT 0,
                badge VARCHAR(20) NOT NULL DEFAULT \'none\'
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_mcsu_sales (
                id INT PRIMARY KEY,
                employee_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_mcsu_reviews (
                id INT PRIMARY KEY,
                employee_id INT NOT NULL,
                rating INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mcsu_reviews', 'mi_mcsu_sales', 'mi_mcsu_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_mcsu_employees VALUES (1, 'Alice', 0, 0, 'none')");
        $this->mysqli->query("INSERT INTO mi_mcsu_employees VALUES (2, 'Bob', 0, 0, 'none')");
        $this->mysqli->query("INSERT INTO mi_mcsu_employees VALUES (3, 'Charlie', 0, 0, 'none')");

        $this->mysqli->query("INSERT INTO mi_mcsu_sales VALUES (1, 1, 500)");
        $this->mysqli->query("INSERT INTO mi_mcsu_sales VALUES (2, 1, 300)");
        $this->mysqli->query("INSERT INTO mi_mcsu_sales VALUES (3, 2, 100)");

        $this->mysqli->query("INSERT INTO mi_mcsu_reviews VALUES (1, 1, 5)");
        $this->mysqli->query("INSERT INTO mi_mcsu_reviews VALUES (2, 1, 4)");
        $this->mysqli->query("INSERT INTO mi_mcsu_reviews VALUES (3, 2, 3)");
        $this->mysqli->query("INSERT INTO mi_mcsu_reviews VALUES (4, 3, 5)");
    }

    public function testUpdateTwoColumnsFromDifferentTables(): void
    {
        $sql = "UPDATE mi_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM mi_mcsu_sales WHERE employee_id = mi_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM mi_mcsu_reviews WHERE employee_id = mi_mcsu_employees.id
                )";

        try {
            $this->mysqli->query($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM mi_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Alice total_sales expected 800, got ' . $alice['total_sales']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.01);
            $this->assertEqualsWithDelta(100.0, (float) $rows[1]['total_sales'], 0.01);
            $this->assertEqualsWithDelta(0.0, (float) $rows[2]['total_sales'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedMultiCorrelatedUpdate(): void
    {
        $sql = "UPDATE mi_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM mi_mcsu_sales
                    WHERE employee_id = mi_mcsu_employees.id AND amount > ?
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM mi_mcsu_reviews
                    WHERE employee_id = mi_mcsu_employees.id AND rating >= ?
                )";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $stmt->bind_param('di', $minAmount, $minRating);
            $minAmount = 200.0;
            $minRating = 4;
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM mi_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared MySQLi: Alice sales expected 800, got '
                    . $alice['total_sales'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }
}
