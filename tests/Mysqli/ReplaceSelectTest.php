<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests REPLACE INTO ... SELECT on MySQLi ZTD.
 *
 * MySQL supports REPLACE INTO ... SELECT to replace/insert rows from a SELECT.
 * The ReplaceTransformer handles this by building SELECT SQL from the
 * statement's select property, and ReplaceMutation handles the shadow store.
 */
class ReplaceSelectTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_rsel_target');
        $raw->query('DROP TABLE IF EXISTS mi_rsel_source');
        $raw->query('CREATE TABLE mi_rsel_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->query('CREATE TABLE mi_rsel_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * REPLACE INTO ... SELECT — all new rows.
     */
    public function testReplaceSelectAllNew(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_rsel_target WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * REPLACE INTO ... SELECT — with existing rows to replace.
     */
    public function testReplaceSelectWithConflict(): void
    {
        $this->mysqli->query("INSERT INTO mi_rsel_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        // id=1 should be replaced
        $result = $this->mysqli->query('SELECT name, score FROM mi_rsel_target WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * REPLACE INTO ... SELECT with WHERE filter.
     */
    public function testReplaceSelectWithWhere(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source WHERE score >= 80');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation: REPLACE INTO ... SELECT stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_rsel_target');
            $raw->query('DROP TABLE IF EXISTS mi_rsel_source');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
