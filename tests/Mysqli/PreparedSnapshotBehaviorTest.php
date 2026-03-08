<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared statement CTE snapshot behavior via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedSnapshotBehaviorTest.
 */
class PreparedSnapshotBehaviorTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_psb_test');
        $raw->query('CREATE TABLE mi_psb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (2, 'Bob', 80)");
    }

    /**
     * INSERT after prepare() is NOT visible.
     */
    public function testInsertAfterPrepareNotVisible(): void
    {
        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Fresh prepare() after INSERT sees new data.
     */
    public function testFreshPrepareSeesNewData(): void
    {
        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Re-execution uses stale snapshot.
     */
    public function testReExecutionUsesStaleSnapshot(): void
    {
        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_psb_test');

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query("INSERT INTO mi_psb_test VALUES (3, 'Charlie', 90)");

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
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
            $raw->query('DROP TABLE IF EXISTS mi_psb_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
