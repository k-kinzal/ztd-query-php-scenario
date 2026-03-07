<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ztdAffectedRows() and affected_rows behavior after write operations in ZTD mode on MySQLi.
 */
class RowCountEdgeCasesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_rc_items');
        $raw->query('CREATE TABLE mi_rc_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), active TINYINT)');
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

        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (1, 'Alpha', 'A', 1)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (2, 'Beta', 'A', 1)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (3, 'Gamma', 'B', 0)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (4, 'Delta', 'B', 1)");
    }

    public function testAffectedRowsAfterPreparedUpdateMultiple(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_rc_items SET active = ? WHERE category = ?');
        $active = 0;
        $category = 'A';
        $stmt->bind_param('is', $active, $category);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsAfterPreparedDeleteMultiple(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_rc_items WHERE category = ?');
        $category = 'B';
        $stmt->bind_param('s', $category);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsAfterUpdateNoMatch(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_rc_items SET name = ? WHERE id = ?');
        $name = 'NoMatch';
        $id = 999;
        $stmt->bind_param('si', $name, $id);
        $stmt->execute();
        $this->assertSame(0, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsReExecuteWithFrozenSnapshot(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_rc_items WHERE category = ?');
        $category = 'A';
        $stmt->bind_param('s', $category);

        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());

        $category = 'B';
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());

        // CTE snapshot is frozen at prepare time — DELETE still "sees" 'A'
        // rows in the snapshot even though they were already deleted.
        $category = 'A';
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
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
        $raw->query('DROP TABLE IF EXISTS mi_rc_items');
        $raw->close();
    }
}
