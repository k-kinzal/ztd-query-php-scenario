<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests DELETE with ORDER BY and LIMIT on MySQL ZTD.
 *
 * MySQL supports: DELETE FROM t WHERE ... ORDER BY ... LIMIT n
 * This allows deleting only the first N rows matching a condition,
 * sorted by the specified order.
 */
class DeleteWithOrderByLimitTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_dol_test');
        $raw->query('CREATE TABLE mi_dol_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_dol_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_dol_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_dol_test (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->mysqli->query("INSERT INTO mi_dol_test (id, name, score) VALUES (4, 'Dave', 60)");
        $this->mysqli->query("INSERT INTO mi_dol_test (id, name, score) VALUES (5, 'Eve', 50)");
    }

    /**
     * DELETE with LIMIT only (no ORDER BY).
     */
    public function testDeleteWithLimit(): void
    {
        $this->mysqli->query('DELETE FROM mi_dol_test LIMIT 2');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dol_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * DELETE with ORDER BY and LIMIT deletes the N lowest-scoring rows.
     */
    public function testDeleteWithOrderByAndLimit(): void
    {
        $this->mysqli->query('DELETE FROM mi_dol_test ORDER BY score ASC LIMIT 2');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dol_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);

        // The two lowest-scoring (Eve=50, Dave=60) should be deleted
        $result = $this->mysqli->query('SELECT name FROM mi_dol_test ORDER BY score ASC');
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Charlie', $names);
    }

    /**
     * DELETE with WHERE + ORDER BY + LIMIT.
     */
    public function testDeleteWithWhereOrderByLimit(): void
    {
        $this->mysqli->query('DELETE FROM mi_dol_test WHERE score < 85 ORDER BY score DESC LIMIT 1');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dol_test');
        $this->assertEquals(4, (int) $result->fetch_assoc()['cnt']);

        // Highest scoring among score < 85 is Bob (80), so Bob should be deleted
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_dol_test WHERE name = 'Bob'");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation for DELETE with ORDER BY + LIMIT.
     */
    public function testDeleteWithOrderByLimitPhysicalIsolation(): void
    {
        $this->mysqli->query('DELETE FROM mi_dol_test ORDER BY score ASC LIMIT 3');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dol_test');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
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
            $raw->query('DROP TABLE IF EXISTS mi_dol_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
