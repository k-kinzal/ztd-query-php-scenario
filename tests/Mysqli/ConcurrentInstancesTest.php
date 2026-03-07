<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests that multiple ZtdMysqli instances connected to the same MySQL database
 * maintain independent shadow stores with interleaved operations.
 */
class ConcurrentInstancesTest extends TestCase
{
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
        $raw->query('DROP TABLE IF EXISTS mi_ci_items');
        $raw->query('CREATE TABLE mi_ci_items (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->close();
    }

    public function testInsertInOneInstanceInvisibleToOther(): void
    {
        $a = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());
        $b = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());

        $a->query("INSERT INTO mi_ci_items (id, name, score) VALUES (1, 'FromA', 50)");

        $resultA = $a->query('SELECT COUNT(*) AS cnt FROM mi_ci_items');
        $this->assertSame(1, (int) $resultA->fetch_assoc()['cnt']);

        $resultB = $b->query('SELECT COUNT(*) AS cnt FROM mi_ci_items');
        $this->assertSame(0, (int) $resultB->fetch_assoc()['cnt']);

        $a->close();
        $b->close();
    }

    public function testInterleavedInsertsBothIndependent(): void
    {
        $a = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());
        $b = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());

        $a->query("INSERT INTO mi_ci_items (id, name, score) VALUES (1, 'A1', 60)");
        $b->query("INSERT INTO mi_ci_items (id, name, score) VALUES (2, 'B1', 70)");
        $a->query("INSERT INTO mi_ci_items (id, name, score) VALUES (3, 'A2', 80)");

        $resultA = $a->query('SELECT COUNT(*) AS cnt FROM mi_ci_items');
        $this->assertSame(2, (int) $resultA->fetch_assoc()['cnt']);

        $resultB = $b->query('SELECT COUNT(*) AS cnt FROM mi_ci_items');
        $this->assertSame(1, (int) $resultB->fetch_assoc()['cnt']);

        $a->close();
        $b->close();
    }

    public function testUpdateIsolation(): void
    {
        $a = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());
        $b = new ZtdMysqli(MySQLContainer::getHost(), 'root', 'root', 'test', MySQLContainer::getPort());

        $a->query("INSERT INTO mi_ci_items (id, name, score) VALUES (1, 'Shared', 100)");
        $b->query("INSERT INTO mi_ci_items (id, name, score) VALUES (1, 'Shared', 100)");

        $a->query("UPDATE mi_ci_items SET name = 'UpdatedByA' WHERE id = 1");

        $nameB = $b->query('SELECT name FROM mi_ci_items WHERE id = 1')->fetch_assoc()['name'];
        $this->assertSame('Shared', $nameB);

        $nameA = $a->query('SELECT name FROM mi_ci_items WHERE id = 1')->fetch_assoc()['name'];
        $this->assertSame('UpdatedByA', $nameA);

        $a->close();
        $b->close();
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
        $raw->query('DROP TABLE IF EXISTS mi_ci_items');
        $raw->close();
    }
}
