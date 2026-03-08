<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared pagination (LIMIT/OFFSET) with shadow mutations via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedPaginationAfterMutationTest (PDO).
 */
class PreparedPaginationAfterMutationTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ppag_test');
        $raw->query('CREATE TABLE mi_ppag_test (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10))');
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

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'A' : 'B';
            $this->mysqli->query("INSERT INTO mi_ppag_test VALUES ($i, 'Item$i', '$cat')");
        }
    }

    /**
     * Prepared LIMIT and OFFSET.
     */
    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_ppag_test ORDER BY id LIMIT ? OFFSET ?');
        $limit = 3;
        $offset = 0;
        $stmt->bind_param('ii', $limit, $offset);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Item1', $rows[0]['name']);
    }

    /**
     * Pagination after INSERT reflects new row.
     */
    public function testPaginationAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_ppag_test VALUES (11, 'Item11', 'A')");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ppag_test');
        $this->assertSame(11, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Pagination after DELETE reduces total.
     */
    public function testPaginationAfterDelete(): void
    {
        $this->mysqli->query("DELETE FROM mi_ppag_test WHERE category = 'B'");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ppag_test');
        $this->assertSame(5, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ppag_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_ppag_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
