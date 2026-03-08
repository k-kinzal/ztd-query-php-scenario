<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests UNION with LIMIT/OFFSET via MySQLi.
 *
 * MySQL's EXCEPT/INTERSECT support is limited (MySQL 8.0.31+).
 * Focus on UNION ALL and UNION with LIMIT/ORDER BY.
 */
class SetOperationsWithLimitTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_set_a');
        $raw->query('DROP TABLE IF EXISTS mi_set_b');
        $raw->query('CREATE TABLE mi_set_a (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->query('CREATE TABLE mi_set_b (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_set_a VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_set_a VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_set_a VALUES (3, 'Charlie', 70)");

        $this->mysqli->query("INSERT INTO mi_set_b VALUES (4, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_set_b VALUES (5, 'Diana', 60)");
        $this->mysqli->query("INSERT INTO mi_set_b VALUES (6, 'Eve', 50)");
    }

    /**
     * UNION ALL with LIMIT.
     */
    public function testUnionAllWithLimit(): void
    {
        $result = $this->mysqli->query('
            SELECT name, score FROM mi_set_a
            UNION ALL
            SELECT name, score FROM mi_set_b
            LIMIT 4
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
    }

    /**
     * UNION with ORDER BY and LIMIT.
     */
    public function testUnionWithOrderByAndLimit(): void
    {
        $result = $this->mysqli->query('
            SELECT name, score FROM mi_set_a
            UNION
            SELECT name, score FROM mi_set_b
            ORDER BY name
            LIMIT 3 OFFSET 1
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        // Sorted: Alice, Bob, Charlie, Diana, Eve → skip 1 → Bob, Charlie, Diana
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * UNION ALL with ORDER BY DESC and LIMIT.
     */
    public function testUnionAllWithOrderByDescAndLimit(): void
    {
        $result = $this->mysqli->query('
            SELECT name, score FROM mi_set_a
            UNION ALL
            SELECT name, score FROM mi_set_b
            ORDER BY score DESC
            LIMIT 3
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(90, (int) $rows[0]['score']);
    }

    /**
     * UNION reflects INSERT mutation.
     */
    public function testUnionReflectsInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_set_a VALUES (7, 'Frank', 95)");

        $result = $this->mysqli->query('
            SELECT name FROM mi_set_a
            UNION ALL
            SELECT name FROM mi_set_b
            ORDER BY name
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Frank', $names);
        $this->assertCount(7, $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_set_a');
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
            $raw->query('DROP TABLE IF EXISTS mi_set_a');
            $raw->query('DROP TABLE IF EXISTS mi_set_b');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
