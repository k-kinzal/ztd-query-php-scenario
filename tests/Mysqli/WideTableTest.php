<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests wide table (many columns) behavior with ZTD via MySQLi.
 *
 * Cross-platform parity with MysqlWideTableTest (PDO).
 */
class WideTableTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $cols = ['id INT PRIMARY KEY'];
        for ($i = 1; $i <= 19; $i++) {
            $cols[] = "col{$i} VARCHAR(50)";
        }
        $ddl = 'CREATE TABLE mi_wide (' . implode(', ', $cols) . ')';

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_wide');
        $raw->query($ddl);
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

    public function testInsertAndSelectAllColumns(): void
    {
        $vals = ['1'];
        for ($i = 1; $i <= 19; $i++) {
            $vals[] = "'val_{$i}'";
        }
        $this->mysqli->query('INSERT INTO mi_wide VALUES (' . implode(', ', $vals) . ')');

        $result = $this->mysqli->query('SELECT * FROM mi_wide WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('val_1', $row['col1']);
        $this->assertSame('val_10', $row['col10']);
        $this->assertSame('val_19', $row['col19']);
        $this->assertSame(20, $result->field_count);
    }

    public function testUpdateWideTable(): void
    {
        $vals = ['1'];
        for ($i = 1; $i <= 19; $i++) {
            $vals[] = "'val_{$i}'";
        }
        $this->mysqli->query('INSERT INTO mi_wide VALUES (' . implode(', ', $vals) . ')');

        $this->mysqli->query("UPDATE mi_wide SET col5 = 'updated_5', col15 = 'updated_15' WHERE id = 1");

        $result = $this->mysqli->query('SELECT col5, col15 FROM mi_wide WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated_5', $row['col5']);
        $this->assertSame('updated_15', $row['col15']);
    }

    public function testPreparedInsertWideTable(): void
    {
        $placeholders = array_fill(0, 20, '?');
        $sql = 'INSERT INTO mi_wide VALUES (' . implode(', ', $placeholders) . ')';
        $stmt = $this->mysqli->prepare($sql);

        $params = [1];
        for ($i = 1; $i <= 19; $i++) {
            $params[] = "prep_val_{$i}";
        }

        $types = 'i' . str_repeat('s', 19);
        $stmt->bind_param($types, ...$params);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT col1, col19 FROM mi_wide WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('prep_val_1', $row['col1']);
        $this->assertSame('prep_val_19', $row['col19']);
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
            $raw->query('DROP TABLE IF EXISTS mi_wide');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
