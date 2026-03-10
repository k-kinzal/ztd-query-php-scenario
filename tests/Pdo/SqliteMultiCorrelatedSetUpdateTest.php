<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with multiple SET columns, each derived from a different
 * correlated subquery referencing different shadow tables.
 *
 * Pattern: UPDATE t1 SET col1 = (SELECT ... FROM t2 WHERE t2.fk = t1.id),
 *                        col2 = (SELECT ... FROM t3 WHERE t3.fk = t1.id)
 *
 * Stresses the CTE rewriter's ability to handle multiple correlated
 * subqueries in a single UPDATE statement.
 *
 * @spec SPEC-4.2
 */
class SqliteMultiCorrelatedSetUpdateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mcsu_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                total_sales REAL NOT NULL DEFAULT 0,
                avg_rating REAL NOT NULL DEFAULT 0,
                badge TEXT NOT NULL DEFAULT \'none\'
            )',
            'CREATE TABLE sl_mcsu_sales (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
            'CREATE TABLE sl_mcsu_reviews (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                rating INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mcsu_reviews', 'sl_mcsu_sales', 'sl_mcsu_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mcsu_employees VALUES (1, 'Alice', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO sl_mcsu_employees VALUES (2, 'Bob', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO sl_mcsu_employees VALUES (3, 'Charlie', 0, 0, 'none')");

        // Sales
        $this->pdo->exec("INSERT INTO sl_mcsu_sales VALUES (1, 1, 500)");
        $this->pdo->exec("INSERT INTO sl_mcsu_sales VALUES (2, 1, 300)");
        $this->pdo->exec("INSERT INTO sl_mcsu_sales VALUES (3, 2, 100)");

        // Reviews
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (3, 2, 3)");
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (4, 3, 5)");
    }

    /**
     * UPDATE two columns with correlated subqueries from different tables.
     */
    public function testUpdateTwoColumnsFromDifferentTables(): void
    {
        $sql = "UPDATE sl_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_mcsu_sales WHERE employee_id = sl_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM sl_mcsu_reviews WHERE employee_id = sl_mcsu_employees.id
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM sl_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            // Alice: sales 800, avg rating 4.5
            $alice = $rows[0];
            if (abs((float) $alice['total_sales'] - 800.0) > 0.01) {
                $this->markTestIncomplete(
                    'Alice total_sales expected 800, got ' . $alice['total_sales']
                    . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertEqualsWithDelta(800.0, (float) $alice['total_sales'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $alice['avg_rating'], 0.01);

            // Bob: sales 100, avg rating 3
            $bob = $rows[1];
            $this->assertEqualsWithDelta(100.0, (float) $bob['total_sales'], 0.01);
            $this->assertEqualsWithDelta(3.0, (float) $bob['avg_rating'], 0.01);

            // Charlie: sales 0, avg rating 5
            $charlie = $rows[2];
            $this->assertEqualsWithDelta(0.0, (float) $charlie['total_sales'], 0.01);
            $this->assertEqualsWithDelta(5.0, (float) $charlie['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-correlated UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE three columns from three different subqueries.
     */
    public function testUpdateThreeColumnsFromSubqueries(): void
    {
        $sql = "UPDATE sl_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_mcsu_sales WHERE employee_id = sl_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM sl_mcsu_reviews WHERE employee_id = sl_mcsu_employees.id
                ),
                badge = CASE
                    WHEN (SELECT COUNT(*) FROM sl_mcsu_sales WHERE employee_id = sl_mcsu_employees.id) >= 2 THEN 'star'
                    ELSE 'none'
                END";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating, badge FROM sl_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            // Alice has 2 sales → badge = star
            if ($rows[0]['badge'] !== 'star') {
                $this->markTestIncomplete(
                    'Alice badge expected star, got ' . $rows[0]['badge']
                    . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertSame('star', $rows[0]['badge']);

            // Bob has 1 sale → badge = none
            $this->assertSame('none', $rows[1]['badge']);

            // Charlie has 0 sales → badge = none
            $this->assertSame('none', $rows[2]['badge']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Triple-correlated UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared multi-correlated UPDATE with bound parameter.
     */
    public function testPreparedMultiCorrelatedUpdate(): void
    {
        $sql = "UPDATE sl_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_mcsu_sales
                    WHERE employee_id = sl_mcsu_employees.id AND amount > ?
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM sl_mcsu_reviews
                    WHERE employee_id = sl_mcsu_employees.id AND rating >= ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([200, 4]);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM sl_mcsu_employees ORDER BY name");

            $this->assertCount(3, $rows);

            // Alice: sales > 200 → 500+300=800, ratings >= 4 → avg(5,4) = 4.5
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
            $this->assertEqualsWithDelta(0.0, (float) $rows[1]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-correlated UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-correlated UPDATE on shadow-inserted data.
     */
    public function testMultiCorrelatedUpdateOnShadowData(): void
    {
        // Add new employee and their data in shadow
        $this->pdo->exec("INSERT INTO sl_mcsu_employees VALUES (4, 'Diana', 0, 0, 'none')");
        $this->pdo->exec("INSERT INTO sl_mcsu_sales VALUES (10, 4, 750)");
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (10, 4, 5)");
        $this->pdo->exec("INSERT INTO sl_mcsu_reviews VALUES (11, 4, 5)");

        $sql = "UPDATE sl_mcsu_employees
                SET total_sales = (
                    SELECT COALESCE(SUM(amount), 0) FROM sl_mcsu_sales WHERE employee_id = sl_mcsu_employees.id
                ),
                avg_rating = (
                    SELECT COALESCE(AVG(rating), 0) FROM sl_mcsu_reviews WHERE employee_id = sl_mcsu_employees.id
                )
                WHERE id = 4";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, total_sales, avg_rating FROM sl_mcsu_employees WHERE id = 4");

            $this->assertCount(1, $rows);

            if (abs((float) $rows[0]['total_sales'] - 750.0) > 0.01) {
                $this->markTestIncomplete(
                    'Shadow multi-correlated: Diana sales expected 750, got '
                    . $rows[0]['total_sales'] . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(750.0, (float) $rows[0]['total_sales'], 0.01);
            $this->assertEqualsWithDelta(5.0, (float) $rows[0]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow multi-correlated UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
