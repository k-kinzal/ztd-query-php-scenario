<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests shadow store behavior with a large dataset (100+ rows) on MySQL.
 *
 * Cross-platform parity with SqliteLargeDatasetTest.
 * Validates CTE-based shadow store doesn't degrade with larger datasets.
 * @spec SPEC-3.1
 */
class MysqlLargeDatasetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_large_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(10))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_large_test'];
    }
    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 100; $i++) {
            $name = "User{$i}";
            $score = $i * 10;
            $cat = chr(65 + ($i % 5)); // A-E
            $this->pdo->exec("INSERT INTO pdo_large_test VALUES ({$i}, '{$name}', {$score}, '{$cat}')");
        }
    }

    /**
     * COUNT all rows.
     */
    public function testCountAllRows(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_large_test');
        $this->assertSame(100, (int) $stmt->fetchColumn());
    }

    /**
     * Aggregation: SUM, AVG, MIN, MAX.
     */
    public function testAggregation(): void
    {
        $stmt = $this->pdo->query('SELECT SUM(score) AS s, AVG(score) AS a, MIN(score) AS mi, MAX(score) AS ma FROM pdo_large_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // SUM = 10+20+...+1000 = 10*sum(1..100) = 10*5050 = 50500
        $this->assertSame(50500, (int) $row['s']);
        $this->assertEqualsWithDelta(505.0, (float) $row['a'], 0.1);
        $this->assertSame(10, (int) $row['mi']);
        $this->assertSame(1000, (int) $row['ma']);
    }

    /**
     * GROUP BY with 5 categories.
     */
    public function testGroupBy(): void
    {
        $stmt = $this->pdo->query('
            SELECT category, COUNT(*) AS cnt
            FROM pdo_large_test
            GROUP BY category
            ORDER BY category
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(20, (int) $rows[0]['cnt']); // Each category has 20 rows
    }

    /**
     * UPDATE subset of rows.
     */
    public function testUpdateSubset(): void
    {
        $this->pdo->exec("UPDATE pdo_large_test SET score = score + 1000 WHERE category = 'A'");

        $stmt = $this->pdo->query("SELECT MIN(score) FROM pdo_large_test WHERE category = 'A'");
        $minScore = (int) $stmt->fetchColumn();
        $this->assertGreaterThanOrEqual(1010, $minScore);
    }

    /**
     * DELETE subset of rows.
     */
    public function testDeleteSubset(): void
    {
        $this->pdo->exec("DELETE FROM pdo_large_test WHERE category = 'E'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_large_test');
        $this->assertSame(80, (int) $stmt->fetchColumn());
    }

    /**
     * ORDER BY with LIMIT and OFFSET.
     */
    public function testOrderByLimitOffset(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_large_test ORDER BY score DESC LIMIT 5 OFFSET 0');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $rows);
        $this->assertSame('User100', $rows[0]);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_large_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
