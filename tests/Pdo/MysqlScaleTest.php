<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests shadow store behavior at higher scale on MySQL PDO.
 */
class MysqlScaleTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS scale_items');
        $raw->exec('CREATE TABLE scale_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
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
    }

    public function testBulkInsertThenAggregation(): void
    {
        for ($i = 1; $i <= 200; $i++) {
            $cat = chr(65 + ($i % 5));
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', '$cat', $i)");
        }

        $stmt = $this->pdo->query('SELECT category, COUNT(*) AS cnt FROM scale_items GROUP BY category ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame(40, (int) $rows[0]['cnt']);
    }

    public function testInterleavedMutationsAndReads(): void
    {
        for ($i = 1; $i <= 50; $i++) {
            $this->pdo->exec("INSERT INTO scale_items (id, name, category, score) VALUES ($i, 'Item$i', 'A', 100)");
        }
        $cnt = (int) $this->pdo->query('SELECT COUNT(*) AS c FROM scale_items')->fetch(PDO::FETCH_ASSOC)['c'];
        $this->assertSame(50, $cnt);

        $this->pdo->exec("UPDATE scale_items SET score = 200 WHERE id <= 25");
        $this->pdo->exec("DELETE FROM scale_items WHERE id <= 12");
        $cnt2 = (int) $this->pdo->query('SELECT COUNT(*) AS c FROM scale_items')->fetch(PDO::FETCH_ASSOC)['c'];
        $this->assertSame(38, $cnt2);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS scale_items');
    }
}
