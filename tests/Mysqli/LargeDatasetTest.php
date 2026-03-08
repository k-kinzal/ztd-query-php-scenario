<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests shadow store with a large dataset (100+ rows) via MySQLi.
 *
 * Cross-platform parity with SqliteLargeDatasetTest and MysqlLargeDatasetTest (PDO).
 * @spec pending
 */
class LargeDatasetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_large_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(10))';
    }

    protected function getTableNames(): array
    {
        return ['mi_large_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 100; $i++) {
            $name = "User{$i}";
            $score = $i * 10;
            $cat = chr(65 + ($i % 5)); // A-E
            $this->mysqli->query("INSERT INTO mi_large_test VALUES ({$i}, '{$name}', {$score}, '{$cat}')");
        }
    }

    /**
     * COUNT all rows.
     */
    public function testCountAllRows(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_large_test');
        $this->assertSame(100, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Aggregation.
     */
    public function testAggregation(): void
    {
        $result = $this->mysqli->query('SELECT SUM(score) AS s, MIN(score) AS mi, MAX(score) AS ma FROM mi_large_test');
        $row = $result->fetch_assoc();
        $this->assertSame(50500, (int) $row['s']);
        $this->assertSame(10, (int) $row['mi']);
        $this->assertSame(1000, (int) $row['ma']);
    }

    /**
     * GROUP BY.
     */
    public function testGroupBy(): void
    {
        $result = $this->mysqli->query('
            SELECT category, COUNT(*) AS cnt
            FROM mi_large_test
            GROUP BY category
            ORDER BY category
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(20, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE subset.
     */
    public function testUpdateSubset(): void
    {
        $this->mysqli->query("UPDATE mi_large_test SET score = score + 1000 WHERE category = 'A'");

        $result = $this->mysqli->query("SELECT MIN(score) AS ms FROM mi_large_test WHERE category = 'A'");
        $this->assertGreaterThanOrEqual(1010, (int) $result->fetch_assoc()['ms']);
    }

    /**
     * DELETE subset.
     */
    public function testDeleteSubset(): void
    {
        $this->mysqli->query("DELETE FROM mi_large_test WHERE category = 'E'");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_large_test');
        $this->assertSame(80, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * ORDER BY with LIMIT.
     */
    public function testOrderByWithLimit(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_large_test ORDER BY score DESC LIMIT 1');
        $this->assertSame('User100', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_large_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
