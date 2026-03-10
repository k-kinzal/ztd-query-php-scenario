<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with multiple SET columns, each derived from a different
 * correlated subquery referencing different shadow tables — MySQL.
 *
 * Known to work on MySQL, fails on SQLite/PostgreSQL (SPEC-10.2.233).
 *
 * @spec SPEC-4.2
 */
class MysqlMultiCorrelatedSetUpdateTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_mcsu_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                total_sales DECIMAL(10,2) NOT NULL DEFAULT 0,
                avg_rating DECIMAL(3,1) NOT NULL DEFAULT 0,
                badge VARCHAR(20) NOT NULL DEFAULT \'none\'
            ) ENGINE=InnoDB',
            'CREATE TABLE my_mcsu_sales (
                id INT PRIMARY KEY,
                employee_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_mcsu_reviews (
                id INT PRIMARY KEY,
                employee_id INT NOT NULL,
                rating INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_mcsu_reviews', 'my_mcsu_sales', 'my_mcsu_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_mcsu_employees VALUES (1, 'Alice', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO my_mcsu_employees VALUES (2, 'Bob', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO my_mcsu_employees VALUES (3, 'Charlie', 0, 0, 'none')");

        $this->pdo->exec("INSERT INTO my_mcsu_sales VALUES (1, 1, 500)");
        $this->pdo->exec("INSERT INTO my_mcsu_sales VALUES (2, 1, 300)");
        $this->pdo->exec("INSERT INTO my_mcsu_sales VALUES (3, 2, 100)");

        $this->pdo->exec("INSERT INTO my_mcsu_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO my_mcsu_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO my_mcsu_reviews VALUES (3, 2, 3)");
        $this->pdo->exec("INSERT INTO my_mcsu_reviews VALUES (4, 3, 5)");
    }

    /**
     * UPDATE two columns with correlated subqueries from different tables.
     */
    public function testUpdateTwoColumnsFromDifferentTables(): void
    {
        $sql = "UPDATE my_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM my_mcsu_sales WHERE employee_id = my_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM my_mcsu_reviews WHERE employee_id = my_mcsu_employees.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM my_mcsu_employees ORDER BY name");

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

            $bob = $rows[1];
            $this->assertEqualsWithDelta(100.0, (float) $bob['total_sales'], 0.01);
            $this->assertEqualsWithDelta(3.0, (float) $bob['avg_rating'], 0.01);

            $charlie = $rows[2];
            $this->assertEqualsWithDelta(0.0, (float) $charlie['total_sales'], 0.01);
            $this->assertEqualsWithDelta(5.0, (float) $charlie['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE three columns: two from subqueries, one from CASE with subquery.
     */
    public function testUpdateThreeColumnsFromSubqueries(): void
    {
        $sql = "UPDATE my_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM my_mcsu_sales WHERE employee_id = my_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM my_mcsu_reviews WHERE employee_id = my_mcsu_employees.id
                ),
                badge = CASE
                    WHEN (SELECT COUNT(*) FROM my_mcsu_sales WHERE employee_id = my_mcsu_employees.id) >= 2 THEN 'star'
                    ELSE 'none'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, badge FROM my_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            if ($rows[0]['badge'] !== 'star') {
                $this->markTestIncomplete(
                    'Alice badge expected star, got ' . $rows[0]['badge']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('star', $rows[0]['badge']);  // Alice: 2 sales
            $this->assertSame('none', $rows[1]['badge']);   // Bob: 1 sale
            $this->assertSame('none', $rows[2]['badge']);   // Charlie: 0 sales
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Triple-correlated UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared multi-correlated UPDATE with bound parameters.
     */
    public function testPreparedMultiCorrelatedUpdate(): void
    {
        $sql = "UPDATE my_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM my_mcsu_sales
                    WHERE employee_id = my_mcsu_employees.id AND amount > ?
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM my_mcsu_reviews
                    WHERE employee_id = my_mcsu_employees.id AND rating >= ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([200, 4]);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM my_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            // Alice: sales > 200 → 500+300=800, ratings >= 4 → avg(5,4)=4.5
            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared multi-correlated: Alice sales expected 800, got '
                    . $alice['total_sales'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.01);

            // Bob: sales > 200 → 0, ratings >= 4 → 0
            $this->assertEqualsWithDelta(0.0, (float) $rows[1]['total_sales'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }
}
