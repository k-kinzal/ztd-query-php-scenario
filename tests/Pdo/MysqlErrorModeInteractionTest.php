<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests interaction between PDO error modes and ZTD shadow store on MySQL.
 * @spec SPEC-4.11
 */
class MysqlErrorModeInteractionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE err_mode_m (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['err_mode_m'];
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
}
