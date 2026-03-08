<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests INSERT INTO ... SELECT FROM the same table on MySQLi.
 *
 * Self-referencing INSERT copies rows from a table back into itself.
 */
class SelfReferencingInsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_sri_test');
        $raw->query('CREATE TABLE mi_sri_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))');
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

    /**
     * Self-referencing INSERT with new IDs.
     */
    public function testSelfReferencingInsertWithNewIds(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");

        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM mi_sri_test'
        );
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
        $this->assertSame(4, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Self-referencing INSERT with WHERE filter.
     */
    public function testSelfReferencingInsertWithFilter(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (3, 'Charlie', 70, 'A')");

        $this->mysqli->query(
            "INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, 'A-copy' FROM mi_sri_test WHERE category = 'A'"
        );
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    /**
     * Self-referencing INSERT doesn't loop infinitely.
     */
    public function testSelfReferencingInsertDoesNotLoop(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");

        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 10, name, score, category FROM mi_sri_test'
        );
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM mi_sri_test'
        );

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_sri_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
