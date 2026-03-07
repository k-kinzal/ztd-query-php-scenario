<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests advanced ALTER TABLE operations on MySQL ZTD via MySQLi:
 * - RENAME TABLE (ALTER TABLE ... RENAME TO ...)
 * - CHANGE COLUMN with existing shadow data
 * - MODIFY COLUMN with existing shadow data
 * - Multiple ALTER operations in sequence
 */
class AlterTableAdvancedTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_alt_adv');
        $raw->query('DROP TABLE IF EXISTS mi_alt_adv_new');
        $raw->query('CREATE TABLE mi_alt_adv (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testRenameTable(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv RENAME TO mi_alt_adv_new');

        $result = $this->mysqli->query('SELECT name FROM mi_alt_adv_new WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testChangeColumnWithData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv CHANGE COLUMN name full_name VARCHAR(100)');

        $this->mysqli->query("INSERT INTO mi_alt_adv (id, full_name, score) VALUES (2, 'Bob', 80)");

        $result = $this->mysqli->query('SELECT full_name FROM mi_alt_adv WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['full_name']);

        $result = $this->mysqli->query('SELECT full_name FROM mi_alt_adv WHERE id = 2');
        $this->assertSame('Bob', $result->fetch_assoc()['full_name']);
    }

    public function testModifyColumnWithData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv MODIFY COLUMN name TEXT');

        $result = $this->mysqli->query('SELECT name FROM mi_alt_adv WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testDropColumnRemovesShadowData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv DROP COLUMN score');

        $result = $this->mysqli->query('SELECT * FROM mi_alt_adv WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertArrayNotHasKey('score', $row);
        $this->assertSame('Alice', $row['name']);
    }

    public function testMultipleAlterOperations(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv ADD COLUMN email VARCHAR(100)');
        $this->mysqli->query('ALTER TABLE mi_alt_adv RENAME COLUMN name TO full_name');

        $this->mysqli->query("INSERT INTO mi_alt_adv (id, full_name, score, email) VALUES (2, 'Bob', 80, 'bob@test.com')");

        $result = $this->mysqli->query('SELECT full_name, email FROM mi_alt_adv WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['full_name']);
        $this->assertSame('bob@test.com', $row['email']);

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_alt_adv');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
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
            $raw->query('DROP TABLE IF EXISTS mi_alt_adv');
            $raw->query('DROP TABLE IF EXISTS mi_alt_adv_new');
            $raw->close();
        } catch (\Exception $e) {
            // Container may be unavailable
        }
    }
}
