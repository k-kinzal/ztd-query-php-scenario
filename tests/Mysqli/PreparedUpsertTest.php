<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests UPSERT and REPLACE with prepared statements on MySQLi.
 */
class PreparedUpsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_prep_upsert');
        $raw->query('CREATE TABLE mi_prep_upsert (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testPreparedUpsertInserts(): void
    {
        $stmt = $this->mysqli->prepare(
            'INSERT INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Prepared UPSERT correctly updates existing rows on duplicate key.
     */
    public function testPreparedUpsertUpdatesExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->mysqli->prepare(
            'INSERT INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)'
        );
        $id = 1;
        $name = 'Updated';
        $score = 200;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Updated', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared REPLACE correctly replaces existing rows.
     */
    public function testPreparedReplaceReplacesExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");

        $stmt = $this->mysqli->prepare('REPLACE INTO mi_prep_upsert (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Replaced';
        $score = 999;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, score FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Replaced', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }

    /**
     * Non-prepared UPSERT via query() works correctly.
     */
    public function testQueryUpsertWorksCorrectly(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Original', 50)");
        $this->mysqli->query("INSERT INTO mi_prep_upsert (id, name, score) VALUES (1, 'Updated', 200) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)");

        $result = $this->mysqli->query('SELECT name FROM mi_prep_upsert WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Updated', $row['name']);
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
        $raw->query('DROP TABLE IF EXISTS mi_prep_upsert');
        $raw->close();
    }
}
