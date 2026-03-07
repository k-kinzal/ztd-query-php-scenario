<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests wide table (many columns) behavior with ZTD on MySQL PDO.
 */
class MysqlWideTableTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        // Build a 20-column table
        $cols = ['id INT PRIMARY KEY'];
        for ($i = 1; $i <= 19; $i++) {
            $cols[] = "col{$i} VARCHAR(50)";
        }
        $ddl = 'CREATE TABLE wide_mysql (' . implode(', ', $cols) . ')';

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS wide_mysql');
        $raw->exec($ddl);
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testInsertAndSelectAllColumns(): void
    {
        $vals = ['1'];
        for ($i = 1; $i <= 19; $i++) {
            $vals[] = "'val_{$i}'";
        }
        $this->pdo->exec('INSERT INTO wide_mysql VALUES (' . implode(', ', $vals) . ')');

        $stmt = $this->pdo->query('SELECT * FROM wide_mysql WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('val_1', $row['col1']);
        $this->assertSame('val_10', $row['col10']);
        $this->assertSame('val_19', $row['col19']);
        $this->assertSame(20, $stmt->columnCount());
    }

    public function testUpdateWideTable(): void
    {
        $vals = ['1'];
        for ($i = 1; $i <= 19; $i++) {
            $vals[] = "'val_{$i}'";
        }
        $this->pdo->exec('INSERT INTO wide_mysql VALUES (' . implode(', ', $vals) . ')');

        $this->pdo->exec("UPDATE wide_mysql SET col5 = 'updated_5', col15 = 'updated_15' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT col5, col15 FROM wide_mysql WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated_5', $row['col5']);
        $this->assertSame('updated_15', $row['col15']);
    }

    public function testPreparedInsertWideTable(): void
    {
        $placeholders = array_fill(0, 20, '?');
        $sql = 'INSERT INTO wide_mysql VALUES (' . implode(', ', $placeholders) . ')';
        $stmt = $this->pdo->prepare($sql);

        $params = [1];
        for ($i = 1; $i <= 19; $i++) {
            $params[] = "prep_val_{$i}";
        }
        $stmt->execute($params);

        $sel = $this->pdo->query('SELECT col1, col19 FROM wide_mysql WHERE id = 1');
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('prep_val_1', $row['col1']);
        $this->assertSame('prep_val_19', $row['col19']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS wide_mysql');
    }
}
