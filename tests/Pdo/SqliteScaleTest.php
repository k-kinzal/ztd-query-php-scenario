<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store behavior at higher scale — 200+ rows with
 * interleaved INSERT/UPDATE/DELETE/SELECT operations.
 * @spec pending
 */
class SqliteScaleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE scale_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['scale_items'];
    }


    public function testBulkInsert200Rows(): void
    {
        for ($i = 1; $i <= 200; $i++) {
            $cat = chr(65 + ($i % 5)); // A-E
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', '$cat', $i)");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM scale_items');
        $this->assertSame(200, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testBulkInsertThenAggregation(): void
    {
        for ($i = 1; $i <= 200; $i++) {
            $cat = chr(65 + ($i % 5));
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', '$cat', $i)");
        }

        $stmt = $this->pdo->query('SELECT category, COUNT(*) AS cnt, SUM(score) AS total FROM scale_items GROUP BY category ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        // Each category gets 40 items (200/5)
        $this->assertSame(40, (int) $rows[0]['cnt']);
    }

    public function testBulkInsertThenBulkUpdate(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'A', $i)");
        }

        // Update all scores
        $affected = $this->pdo->exec("UPDATE scale_items SET score = score + 1000 WHERE category = 'A'");
        $this->assertSame(100, $affected);

        // Verify minimum score is now > 1000
        $stmt = $this->pdo->query('SELECT MIN(score) AS min_score FROM scale_items');
        $this->assertGreaterThan(1000, (int) $stmt->fetch(PDO::FETCH_ASSOC)['min_score']);
    }

    public function testBulkInsertThenBulkDelete(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $cat = $i <= 50 ? 'A' : 'B';
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', '$cat', $i)");
        }

        $affected = $this->pdo->exec("DELETE FROM scale_items WHERE category = 'A'");
        $this->assertSame(50, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM scale_items');
        $this->assertSame(50, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testPreparedBulkInsert200Rows(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO scale_items (id, name, category, score) VALUES (?, ?, ?, ?)');
        for ($i = 1; $i <= 200; $i++) {
            $cat = chr(65 + ($i % 3));
            $stmt->execute([$i, "Prep$i", $cat, $i * 10]);
        }

        $count = $this->pdo->query('SELECT COUNT(*) AS cnt FROM scale_items');
        $this->assertSame(200, (int) $count->fetch(PDO::FETCH_ASSOC)['cnt']);

        $sum = $this->pdo->query('SELECT SUM(score) AS total FROM scale_items');
        $expected = array_sum(range(1, 200)) * 10;
        $this->assertSame($expected, (int) $sum->fetch(PDO::FETCH_ASSOC)['total']);
    }

    public function testInterleavedMutationsAndReads(): void
    {
        // Insert 50 items
        for ($i = 1; $i <= 50; $i++) {
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'A', 100)");
        }
        $cnt = $this->pdo->query('SELECT COUNT(*) AS c FROM scale_items')->fetch(PDO::FETCH_ASSOC)['c'];
        $this->assertSame(50, (int) $cnt);

        // Update half
        $this->pdo->exec("UPDATE scale_items SET score = 200 WHERE id <= 25");
        $sum = $this->pdo->query('SELECT SUM(score) AS s FROM scale_items')->fetch(PDO::FETCH_ASSOC)['s'];
        $this->assertSame(25 * 200 + 25 * 100, (int) $sum);

        // Delete a quarter
        $this->pdo->exec("DELETE FROM scale_items WHERE id <= 12");
        $cnt2 = $this->pdo->query('SELECT COUNT(*) AS c FROM scale_items')->fetch(PDO::FETCH_ASSOC)['c'];
        $this->assertSame(38, (int) $cnt2);

        // Insert more
        for ($i = 51; $i <= 75; $i++) {
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'B', 300)");
        }
        $cnt3 = $this->pdo->query('SELECT COUNT(*) AS c FROM scale_items')->fetch(PDO::FETCH_ASSOC)['c'];
        $this->assertSame(63, (int) $cnt3);

        // Final aggregation
        $result = $this->pdo->query('SELECT category, COUNT(*) AS cnt, SUM(score) AS total FROM scale_items GROUP BY category ORDER BY category');
        $rows = $result->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertSame('B', $rows[1]['category']);
        $this->assertSame(25, (int) $rows[1]['cnt']);
    }
}
