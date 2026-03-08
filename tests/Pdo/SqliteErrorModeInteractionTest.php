<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests interaction between PDO error modes and ZTD shadow store.
 *
 * Many applications use ERRMODE_WARNING or ERRMODE_SILENT instead of ERRMODE_EXCEPTION.
 * This tests that ZTD works correctly with all three error modes.
 * @spec SPEC-4.11
 */
class SqliteErrorModeInteractionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['err_mode'];
    }

    public function testErrmodeExceptionThrowsOnInvalidSql(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");

        $this->expectException(\PDOException::class);
        $pdo->query('SELECT * FROM nonexistent_table');
    }

    public function testErrmodeWarningReturnsFalseOnInvalidQuery(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_WARNING,
        ]);
        $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");

        // With ERRMODE_WARNING, query() returns false on error (and emits a warning)
        $result = @$pdo->query('SELECT * FROM nonexistent_table');
        $this->assertFalse($result);

        // Shadow data should still be intact
        $stmt = $pdo->query('SELECT name FROM err_mode WHERE id = 1');
        $this->assertNotFalse($stmt);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testErrmodeSilentReturnsFalseOnInvalidQuery(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT,
        ]);
        $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");

        // With ERRMODE_SILENT, query() returns false on error (no warning)
        $result = $pdo->query('SELECT * FROM nonexistent_table');
        $this->assertFalse($result);

        // Shadow data should still be intact
        $stmt = $pdo->query('SELECT name FROM err_mode WHERE id = 1');
        $this->assertNotFalse($stmt);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testNormalOperationsWorkInAllModes(): void
    {
        foreach ([PDO::ERRMODE_EXCEPTION, PDO::ERRMODE_WARNING, PDO::ERRMODE_SILENT] as $mode) {
            $pdo = new ZtdPdo('sqlite::memory:', null, null, [
                PDO::ATTR_ERRMODE => $mode,
            ]);
            $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
            $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");
            $pdo->exec("INSERT INTO err_mode VALUES (2, 'Bob')");
            $pdo->exec("UPDATE err_mode SET name = 'Updated' WHERE id = 1");

            $stmt = $pdo->query('SELECT name FROM err_mode WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Updated', $row['name'], "Failed with error mode $mode");

            $count = $pdo->query('SELECT COUNT(*) FROM err_mode')->fetchColumn();
            $this->assertSame(2, (int) $count, "Count failed with error mode $mode");
        }
    }

    public function testSwitchingErrorModeMidSession(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");

        // Switch to silent mode
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);

        // Error should be silent now
        $result = $pdo->query('SELECT * FROM nonexistent_table');
        $this->assertFalse($result);

        // Shadow data still intact
        $stmt = $pdo->query('SELECT name FROM err_mode WHERE id = 1');
        $this->assertNotFalse($stmt);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        // Switch back to exception mode
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->expectException(\PDOException::class);
        $pdo->query('SELECT * FROM nonexistent_table');
    }

    public function testPreparedStatementWithSilentMode(): void
    {
        $pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT,
        ]);
        $pdo->exec('CREATE TABLE err_mode (id INT PRIMARY KEY, name VARCHAR(50))');
        $pdo->exec("INSERT INTO err_mode VALUES (1, 'Alice')");

        // Prepared statement should work normally in silent mode
        $stmt = $pdo->prepare('SELECT name FROM err_mode WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }
}
