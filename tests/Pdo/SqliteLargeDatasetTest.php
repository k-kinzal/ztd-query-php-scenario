<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store behavior with larger datasets.
 *
 * Verifies that the CTE-based shadow store handles hundreds of rows
 * correctly — including INSERT, SELECT with aggregation, UPDATE,
 * DELETE, and complex queries on non-trivial data volumes.
 * @spec pending
 */
class SqliteLargeDatasetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ld_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, category TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['ld_test'];
    }


    /**
     * Insert and query 100 rows.
     */
    public function testInsert100Rows(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $cat = ($i % 3 === 0) ? 'A' : (($i % 3 === 1) ? 'B' : 'C');
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, '{$cat}')");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ld_test');
        $this->assertSame(100, (int) $stmt->fetchColumn());
    }

    /**
     * Aggregation on 100 rows.
     */
    public function testAggregationOnLargeDataset(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $cat = ($i % 3 === 0) ? 'A' : (($i % 3 === 1) ? 'B' : 'C');
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, '{$cat}')");
        }

        // SUM of 1..100 = 5050
        $stmt = $this->pdo->query('SELECT SUM(score) FROM ld_test');
        $this->assertSame(5050, (int) $stmt->fetchColumn());

        // AVG
        $stmt = $this->pdo->query('SELECT AVG(score) FROM ld_test');
        $this->assertEqualsWithDelta(50.5, (float) $stmt->fetchColumn(), 0.01);

        // MIN / MAX
        $stmt = $this->pdo->query('SELECT MIN(score), MAX(score) FROM ld_test');
        $row = $stmt->fetch(PDO::FETCH_NUM);
        $this->assertSame(1, (int) $row[0]);
        $this->assertSame(100, (int) $row[1]);
    }

    /**
     * GROUP BY on 100 rows.
     */
    public function testGroupByOnLargeDataset(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $cat = ($i % 3 === 0) ? 'A' : (($i % 3 === 1) ? 'B' : 'C');
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, '{$cat}')");
        }

        $stmt = $this->pdo->query('SELECT category, COUNT(*) AS cnt FROM ld_test GROUP BY category ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        // A: multiples of 3 (3,6,9,...,99) = 33 rows
        // B: i%3==1 (1,4,7,...,100) = 34 rows
        // C: i%3==2 (2,5,8,...,98) = 33 rows
        $this->assertSame('A', $rows[0]['category']);
        $this->assertSame(33, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE subset of 100 rows.
     */
    public function testUpdateSubsetOfLargeDataset(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, 'X')");
        }

        // Update rows with score > 90
        $affected = $this->pdo->exec('UPDATE ld_test SET category = \'TOP\' WHERE score > 90');
        $this->assertSame(10, $affected);

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM ld_test WHERE category = 'TOP'");
        $this->assertSame(10, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE subset of 100 rows.
     */
    public function testDeleteSubsetOfLargeDataset(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, 'X')");
        }

        // Delete rows with score <= 50
        $affected = $this->pdo->exec('DELETE FROM ld_test WHERE score <= 50');
        $this->assertSame(50, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ld_test');
        $this->assertSame(50, (int) $stmt->fetchColumn());
    }

    /**
     * Multi-row INSERT (bulk) then query.
     */
    public function testMultiRowBulkInsert(): void
    {
        // Build a multi-row INSERT with 50 rows
        $values = [];
        for ($i = 1; $i <= 50; $i++) {
            $values[] = "({$i}, 'Bulk{$i}', {$i}, 'BULK')";
        }
        $sql = 'INSERT INTO ld_test (id, name, score, category) VALUES ' . implode(', ', $values);
        $affected = $this->pdo->exec($sql);
        $this->assertSame(50, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ld_test');
        $this->assertSame(50, (int) $stmt->fetchColumn());
    }

    /**
     * ORDER BY with LIMIT and OFFSET on larger dataset.
     */
    public function testOrderByLimitOffsetOnLargeDataset(): void
    {
        for ($i = 1; $i <= 50; $i++) {
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, 'X')");
        }

        // Get rows 11-20 by score descending
        $stmt = $this->pdo->query('SELECT id, score FROM ld_test ORDER BY score DESC LIMIT 10 OFFSET 10');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(10, $rows);
        $this->assertSame(40, (int) $rows[0]['score']); // 50-10=40
        $this->assertSame(31, (int) $rows[9]['score']); // 50-19=31
    }

    /**
     * Physical isolation — 100 shadow rows, 0 physical rows.
     */
    public function testPhysicalIsolation(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->pdo->exec("INSERT INTO ld_test (id, name, score, category) VALUES ({$i}, 'User{$i}', {$i}, 'X')");
        }

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ld_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
