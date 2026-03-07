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
 * Tests interaction between PDO error modes and ZTD shadow store on MySQL.
 */
class MysqlErrorModeInteractionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS err_mode_m');
        $raw->exec('CREATE TABLE err_mode_m (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    public function testErrmodeSilentReturnsFalseOnInvalidQuery(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT],
        );
        $pdo->exec("INSERT INTO err_mode_m VALUES (1, 'Alice')");

        $result = $pdo->query('SELECT * FROM nonexistent_table_xyz');
        $this->assertFalse($result);

        // Shadow data still intact
        $stmt = $pdo->query('SELECT name FROM err_mode_m WHERE id = 1');
        $this->assertNotFalse($stmt);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testNormalOperationsWorkInSilentMode(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT],
        );
        $pdo->exec("INSERT INTO err_mode_m VALUES (1, 'Alice')");
        $pdo->exec("INSERT INTO err_mode_m VALUES (2, 'Bob')");
        $pdo->exec("UPDATE err_mode_m SET name = 'Updated' WHERE id = 1");

        $stmt = $pdo->query('SELECT name FROM err_mode_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated', $row['name']);
    }

    public function testSwitchingErrorModeMidSession(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT],
        );
        $pdo->exec("INSERT INTO err_mode_m VALUES (1, 'Alice')");

        // Silent mode: error returns false
        $result = $pdo->query('SELECT * FROM nonexistent_xyz');
        $this->assertFalse($result);

        // Switch to exception mode
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->expectException(\PDOException::class);
        $pdo->query('SELECT * FROM nonexistent_xyz');
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS err_mode_m');
    }
}
