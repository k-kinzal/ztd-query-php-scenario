<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests shadow store behavior at higher scale on MySQLi.
 */
class ScaleTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_scale_items');
        $raw->query('CREATE TABLE mi_scale_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), score INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testPreparedBulkInsert200Rows(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_scale_items (id, name, category, score) VALUES (?, ?, ?, ?)');
        $id = 0;
        $name = '';
        $cat = '';
        $score = 0;
        $stmt->bind_param('issi', $id, $name, $cat, $score);
        for ($i = 1; $i <= 200; $i++) {
            $id = $i;
            $name = "Prep$i";
            $cat = chr(65 + ($i % 3));
            $score = $i * 10;
            $stmt->execute();
        }

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_scale_items');
        $row = $result->fetch_assoc();
        $this->assertSame(200, (int) $row['cnt']);
    }

    public function testBulkInsertThenAggregation(): void
    {
        for ($i = 1; $i <= 200; $i++) {
            $cat = chr(65 + ($i % 5));
            $this->mysqli->query("INSERT INTO mi_scale_items (id, name, category, score) VALUES ($i, 'Item$i', '$cat', $i)");
        }

        $result = $this->mysqli->query('SELECT category, COUNT(*) AS cnt FROM mi_scale_items GROUP BY category ORDER BY category');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(40, (int) $rows[0]['cnt']);
    }

    public function testInterleavedMutationsAndReads(): void
    {
        for ($i = 1; $i <= 50; $i++) {
            $this->mysqli->query("INSERT INTO mi_scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'A', 100)");
        }
        $result = $this->mysqli->query('SELECT COUNT(*) AS c FROM mi_scale_items');
        $cnt = (int) $result->fetch_assoc()['c'];
        $this->assertSame(50, $cnt);

        $this->mysqli->query("UPDATE mi_scale_items SET score = 200 WHERE id <= 25");
        $this->mysqli->query("DELETE FROM mi_scale_items WHERE id <= 12");
        $result2 = $this->mysqli->query('SELECT COUNT(*) AS c FROM mi_scale_items');
        $cnt2 = (int) $result2->fetch_assoc()['c'];
        $this->assertSame(38, $cnt2);
    }

    public function testBulkUpdateAndVerify(): void
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->mysqli->query("INSERT INTO mi_scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'X', $i)");
        }

        $this->mysqli->query("UPDATE mi_scale_items SET score = score + 1000 WHERE id <= 50");

        $result = $this->mysqli->query('SELECT MIN(score) AS min_score FROM mi_scale_items WHERE id <= 50');
        $row = $result->fetch_assoc();
        $this->assertSame(1001, (int) $row['min_score']);

        $result2 = $this->mysqli->query('SELECT MAX(score) AS max_score FROM mi_scale_items WHERE id > 50');
        $row2 = $result2->fetch_assoc();
        $this->assertSame(100, (int) $row2['max_score']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_scale_items');
        $raw->close();
    }
}
