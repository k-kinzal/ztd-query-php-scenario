<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests shadow store behavior with a large dataset (100+ rows) on PostgreSQL.
 *
 * Cross-platform parity with SqliteLargeDatasetTest.
 * @spec pending
 */
class PostgresLargeDatasetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_large_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(10))';
    }

    protected function getTableNames(): array
    {
        return ['pg_large_test'];
    }
    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 100; $i++) {
            $name = "User{$i}";
            $score = $i * 10;
            $cat = chr(65 + ($i % 5)); // A-E
            $this->pdo->exec("INSERT INTO pg_large_test VALUES ({$i}, '{$name}', {$score}, '{$cat}')");
        }
    }

    /**
     * COUNT all rows.
     */
    public function testCountAllRows(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_large_test');
        $this->assertSame(100, (int) $stmt->fetchColumn());
    }

    /**
     * Aggregation.
     */
    public function testAggregation(): void
    {
        $stmt = $this->pdo->query('SELECT SUM(score) AS s, MIN(score) AS mi, MAX(score) AS ma FROM pg_large_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(50500, (int) $row['s']);
        $this->assertSame(10, (int) $row['mi']);
        $this->assertSame(1000, (int) $row['ma']);
    }

    /**
     * GROUP BY.
     */
    public function testGroupBy(): void
    {
        $stmt = $this->pdo->query('
            SELECT category, COUNT(*) AS cnt
            FROM pg_large_test
            GROUP BY category
            ORDER BY category
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(20, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE subset.
     */
    public function testUpdateSubset(): void
    {
        $this->pdo->exec("UPDATE pg_large_test SET score = score + 1000 WHERE category = 'A'");

        $stmt = $this->pdo->query("SELECT MIN(score) FROM pg_large_test WHERE category = 'A'");
        $this->assertGreaterThanOrEqual(1010, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE subset.
     */
    public function testDeleteSubset(): void
    {
        $this->pdo->exec("DELETE FROM pg_large_test WHERE category = 'E'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_large_test');
        $this->assertSame(80, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_large_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
