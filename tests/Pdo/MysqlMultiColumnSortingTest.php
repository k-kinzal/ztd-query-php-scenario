<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-column ORDER BY with expressions, common in data table UIs.
 * Covers composite sorting, CASE-based priority sorting, expression-based
 * ORDER BY, NULL handling in sorts, and interactions with mutations.
 * @spec SPEC-3.1
 */
class MysqlMultiColumnSortingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_mcs_products (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(50) NOT NULL,
            priority VARCHAR(20),
            price DECIMAL(10,2) NOT NULL,
            rating DECIMAL(3,2),
            stock INT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_mcs_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (1, 'Laptop', 'electronics', 'high', 999.99, 4.50, 25)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (2, 'Mouse', 'electronics', 'low', 29.99, 4.20, 150)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (3, 'Desk', 'furniture', 'medium', 599.99, 4.80, 10)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (4, 'Chair', 'furniture', 'high', 899.99, NULL, 8)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (5, 'Monitor', 'electronics', 'high', 449.99, 4.60, 30)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (6, 'Keyboard', 'electronics', 'medium', 79.99, 4.00, 200)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (7, 'Lamp', 'furniture', 'low', 45.99, NULL, 80)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (8, 'Cable', 'accessories', 'low', 9.99, 3.50, 500)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (9, 'Webcam', 'electronics', 'medium', 89.99, 4.10, 60)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (10, 'Headphones', 'electronics', 'high', 199.99, 4.70, 35)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (11, 'Shelf', 'furniture', NULL, 149.99, 3.90, 20)");
        $this->pdo->exec("INSERT INTO mp_mcs_products VALUES (12, 'Case', 'accessories', NULL, 19.99, NULL, 300)");
    }

    /**
     * Two-column sort: ORDER BY category ASC, price DESC.
     */
    public function testTwoColumnSort(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, category, price FROM mp_mcs_products ORDER BY category ASC, price DESC"
        );

        $this->assertCount(12, $rows);

        // accessories first (alphabetically), sorted by price DESC
        $this->assertSame('accessories', $rows[0]['category']);
        $this->assertSame('Case', $rows[0]['name']); // 19.99
        $this->assertSame('accessories', $rows[1]['category']);
        $this->assertSame('Cable', $rows[1]['name']); // 9.99

        // electronics next, sorted by price DESC
        $this->assertSame('electronics', $rows[2]['category']);
        $this->assertSame('Laptop', $rows[2]['name']); // 999.99

        // furniture last
        $furnitureRows = array_values(array_filter($rows, fn($r) => $r['category'] === 'furniture'));
        $this->assertSame('Chair', $furnitureRows[0]['name']); // 899.99 (highest price)
    }

    /**
     * Three-column sort: category ASC, priority ASC, name ASC.
     */
    public function testThreeColumnSort(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, category, priority, price FROM mp_mcs_products
             WHERE priority IS NOT NULL
             ORDER BY category ASC, priority ASC, name ASC"
        );

        // accessories with priority: Cable (low)
        $this->assertSame('Cable', $rows[0]['name']);
        $this->assertSame('accessories', $rows[0]['category']);

        // electronics: high (Headphones, Laptop, Monitor), low (Mouse), medium (Keyboard, Webcam)
        $electronics = array_values(array_filter($rows, fn($r) => $r['category'] === 'electronics'));
        $this->assertSame('Headphones', $electronics[0]['name']); // high, first alphabetically
        $this->assertSame('Laptop', $electronics[1]['name']);      // high
        $this->assertSame('Monitor', $electronics[2]['name']);     // high
        $this->assertSame('Mouse', $electronics[3]['name']);       // low
        $this->assertSame('Keyboard', $electronics[4]['name']);    // medium
        $this->assertSame('Webcam', $electronics[5]['name']);      // medium
    }

    /**
     * CASE-based priority sorting: custom sort order for enum-like values.
     */
    public function testCaseBasedPrioritySorting(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, priority FROM mp_mcs_products
             WHERE priority IS NOT NULL
             ORDER BY CASE priority
                 WHEN 'high' THEN 1
                 WHEN 'medium' THEN 2
                 WHEN 'low' THEN 3
                 ELSE 4
             END ASC, name ASC"
        );

        // High priority items first
        $this->assertSame('high', $rows[0]['priority']);
        $this->assertSame('Chair', $rows[0]['name']);   // high, alphabetically first
        $this->assertSame('Headphones', $rows[1]['name']); // high
        $this->assertSame('Laptop', $rows[2]['name']);  // high
        $this->assertSame('Monitor', $rows[3]['name']); // high

        // Medium priority items next
        $this->assertSame('medium', $rows[4]['priority']);
        $this->assertSame('Desk', $rows[4]['name']);

        // Low priority items last
        $this->assertSame('low', $rows[7]['priority']);
    }

    /**
     * ORDER BY expression: CHAR_LENGTH(name).
     */
    public function testOrderByLengthExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, CHAR_LENGTH(name) AS name_len FROM mp_mcs_products ORDER BY CHAR_LENGTH(name) ASC, name ASC LIMIT 5"
        );

        $this->assertCount(5, $rows);
        // Shortest names first: Desk (4), Lamp (4), Case (4), Mouse (5), Cable (5)
        $this->assertTrue((int) $rows[0]['name_len'] <= (int) $rows[1]['name_len']);
        $this->assertTrue((int) $rows[1]['name_len'] <= (int) $rows[2]['name_len']);
    }

    /**
     * ORDER BY expression: COALESCE for NULL rating handling.
     */
    public function testOrderByCoalesceExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, rating, COALESCE(rating, 0) AS sort_rating
             FROM mp_mcs_products
             ORDER BY COALESCE(rating, 0) DESC"
        );

        $this->assertCount(12, $rows);
        // Highest rated first (Desk 4.80)
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertEqualsWithDelta(4.8, (float) $rows[0]['sort_rating'], 0.01);

        // NULL ratings last (treated as 0)
        $lastThree = array_slice($rows, -3);
        foreach ($lastThree as $row) {
            $this->assertEqualsWithDelta(0.0, (float) $row['sort_rating'], 0.01);
        }
    }

    /**
     * ORDER BY with NULLS: NULL values in sort column.
     * MySQL sorts NULLs first in ASC order (same as SQLite).
     */
    public function testOrderByWithNulls(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, rating FROM mp_mcs_products ORDER BY rating ASC"
        );

        $this->assertCount(12, $rows);

        // In MySQL, NULL sorts first in ASC order
        $this->assertNull($rows[0]['rating']);
        $this->assertNull($rows[1]['rating']);
        $this->assertNull($rows[2]['rating']);

        // Non-null ratings follow in ascending order
        $this->assertEqualsWithDelta(3.5, (float) $rows[3]['rating'], 0.01);
    }

    /**
     * Sort direction change after UPDATE -- verify sort reflects updated values.
     */
    public function testSortDirectionChangeAfterUpdate(): void
    {
        // Verify initial sort
        $before = $this->ztdQuery(
            "SELECT name, price FROM mp_mcs_products WHERE category = 'electronics' ORDER BY price ASC LIMIT 1"
        );
        $this->assertSame('Mouse', $before[0]['name']); // 29.99 is cheapest

        // Update Mouse price to be the most expensive
        $this->pdo->exec("UPDATE mp_mcs_products SET price = 2000.00 WHERE id = 2");

        // Verify sort now reflects the updated price
        $after = $this->ztdQuery(
            "SELECT name, price FROM mp_mcs_products WHERE category = 'electronics' ORDER BY price ASC LIMIT 1"
        );
        $this->assertSame('Keyboard', $after[0]['name']); // 79.99 is now cheapest

        // Verify Mouse is now highest priced
        $highest = $this->ztdQuery(
            "SELECT name, price FROM mp_mcs_products WHERE category = 'electronics' ORDER BY price DESC LIMIT 1"
        );
        $this->assertSame('Mouse', $highest[0]['name']);
        $this->assertEqualsWithDelta(2000.0, (float) $highest[0]['price'], 0.01);
    }

    /**
     * Prepared statement with ORDER BY and LIMIT.
     * MySQL PDO requires PDO::PARAM_INT binding for LIMIT.
     */
    public function testPreparedOrderByWithLimit(): void
    {
        $stmt = $this->ztdPrepare(
            "SELECT name, price FROM mp_mcs_products WHERE category = ? ORDER BY price DESC LIMIT ?"
        );

        // Top 3 electronics by price
        $stmt->bindValue(1, 'electronics', PDO::PARAM_STR);
        $stmt->bindValue(2, 3, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Laptop', $rows[0]['name']); // 999.99
        $this->assertSame('Monitor', $rows[1]['name']); // 449.99
        $this->assertSame('Headphones', $rows[2]['name']); // 199.99
    }

    /**
     * Physical isolation -- shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_mcs_products");
        $this->assertSame(12, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mp_mcs_products");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
