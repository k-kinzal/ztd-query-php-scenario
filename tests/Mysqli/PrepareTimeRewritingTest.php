<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests that query rewriting occurs at prepare time, not execute time (MySQLi).
 */
class PrepareTimeRewritingTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ptr_items');
        $raw->query('CREATE TABLE mi_ptr_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))');
        $raw->query("INSERT INTO mi_ptr_items VALUES (1, 'Physical A', 10.00)");
        $raw->query("INSERT INTO mi_ptr_items VALUES (2, 'Physical B', 20.00)");
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

    public function testSelectPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->mysqli->query("INSERT INTO mi_ptr_items VALUES (10, 'Shadow X', 99.99)");

        $stmt = $this->mysqli->prepare('SELECT * FROM mi_ptr_items WHERE id = ?');
        $id = 10;
        $stmt->bind_param('i', $id);

        $this->mysqli->disableZtd();

        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Shadow X', $rows[0]['name']);

        $this->mysqli->enableZtd();
    }

    public function testSelectPreparedWithZtdDisabledEnabledBeforeExecute(): void
    {
        $this->mysqli->disableZtd();

        $stmt = $this->mysqli->prepare('SELECT * FROM mi_ptr_items ORDER BY id');

        $this->mysqli->enableZtd();

        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Physical A', $rows[0]['name']);
        $this->assertSame('Physical B', $rows[1]['name']);
    }

    public function testTwoPreparedStatementsOppositeToggle(): void
    {
        $this->mysqli->query("INSERT INTO mi_ptr_items VALUES (10, 'Shadow Only', 50.00)");

        $stmtShadow = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_ptr_items');

        $this->mysqli->disableZtd();
        $stmtPhysical = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM mi_ptr_items');

        $this->mysqli->enableZtd();

        $stmtShadow->execute();
        $result = $stmtShadow->get_result();
        $shadowCount = (int) $result->fetch_assoc()['cnt'];

        $stmtPhysical->execute();
        $result = $stmtPhysical->get_result();
        $physicalCount = (int) $result->fetch_assoc()['cnt'];

        $this->assertSame(1, $shadowCount);
        $this->assertSame(2, $physicalCount);
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
        $raw->query('DROP TABLE IF EXISTS mi_ptr_items');
        $raw->close();
    }
}
