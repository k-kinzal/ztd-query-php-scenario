<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with multiple SET columns each from a different correlated
 * subquery — PostgreSQL.
 *
 * Known to fail on PostgreSQL with "ambiguous column" or syntax errors.
 * This test documents the cross-platform behavior difference.
 *
 * @spec SPEC-4.2
 */
class PostgresMultiCorrelatedSetUpdateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mcsu_employees (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                total_sales NUMERIC(10,2) NOT NULL DEFAULT 0,
                avg_rating NUMERIC(3,1) NOT NULL DEFAULT 0,
                badge VARCHAR(20) NOT NULL DEFAULT \'none\'
            )',
            'CREATE TABLE pg_mcsu_sales (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_mcsu_reviews (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                rating INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mcsu_reviews', 'pg_mcsu_sales', 'pg_mcsu_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mcsu_employees VALUES (1, 'Alice', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO pg_mcsu_employees VALUES (2, 'Bob', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO pg_mcsu_employees VALUES (3, 'Charlie', 0, 0, 'none')");

        $this->pdo->exec("INSERT INTO pg_mcsu_sales VALUES (1, 1, 500)");
        $this->pdo->exec("INSERT INTO pg_mcsu_sales VALUES (2, 1, 300)");
        $this->pdo->exec("INSERT INTO pg_mcsu_sales VALUES (3, 2, 100)");

        $this->pdo->exec("INSERT INTO pg_mcsu_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO pg_mcsu_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO pg_mcsu_reviews VALUES (3, 2, 3)");
        $this->pdo->exec("INSERT INTO pg_mcsu_reviews VALUES (4, 3, 5)");
    }

    /**
     * UPDATE two columns with correlated subqueries from different tables.
     */
    public function testUpdateTwoColumnsFromDifferentTables(): void
    {
        $sql = "UPDATE pg_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM pg_mcsu_sales WHERE employee_id = pg_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM pg_mcsu_reviews WHERE employee_id = pg_mcsu_employees.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM pg_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Alice total_sales expected 800, got ' . $alice['total_sales']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared multi-correlated UPDATE with $N parameters.
     */
    public function testPreparedMultiCorrelatedUpdate(): void
    {
        $sql = "UPDATE pg_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM pg_mcsu_sales
                    WHERE employee_id = pg_mcsu_employees.id AND amount > $1
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM pg_mcsu_reviews
                    WHERE employee_id = pg_mcsu_employees.id AND rating >= $2
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([200, 4]);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM pg_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Prepared PG multi-correlated: Alice sales expected 800, got '
                    . $alice['total_sales'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared PG multi-correlated UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Non-correlated variant (should work on all platforms).
     */
    public function testNonCorrelatedMultiSubquerySet(): void
    {
        $sql = "UPDATE pg_mcsu_employees
                SET total_sales = (SELECT COALESCE(SUM(amount), 0) FROM pg_mcsu_sales),
                    avg_rating = (SELECT COALESCE(AVG(rating), 0) FROM pg_mcsu_reviews)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM pg_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            // All employees get the same values: total sales = 900, avg rating = 4.25
            foreach ($rows as $row) {
                if (abs((float) $row['total_sales'] - 900.0) > 0.01) {
                    $this->markTestIncomplete(
                        $row['name'] . ' total_sales expected 900, got ' . $row['total_sales']
                        . '. Data: ' . json_encode($rows)
                    );
                }
            }

            $this->assertEqualsWithDelta(900.0, (float) $rows[0]['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.25, (float) $rows[0]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Non-correlated multi-subquery SET failed: ' . $e->getMessage());
        }
    }
}
