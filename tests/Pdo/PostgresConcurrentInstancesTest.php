<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use Tests\Support\PostgreSQLContainer;

/**
 * Tests that multiple ZtdPdo instances connected to the same PostgreSQL database
 * maintain independent shadow stores with interleaved operations.
 * @spec SPEC-2.4
 */
class PostgresConcurrentInstancesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ci_items (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['ci_items'];
    }


    public function testInsertInOneInstanceInvisibleToOther(): void
    {
        $pdoA = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $pdoB = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        $pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'FromA', 50)");

        $stmtA = $pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(1, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        $stmtB = $pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testInterleavedInsertsBothIndependent(): void
    {
        $pdoA = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $pdoB = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        $pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'A1', 60)");
        $pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (2, 'B1', 70)");
        $pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (3, 'A2', 80)");

        $stmtA = $pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(2, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        $stmtB = $pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(1, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testUpdateIsolation(): void
    {
        $pdoA = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $pdoB = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        $pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'Shared', 100)");
        $pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'Shared', 100)");

        $pdoA->exec("UPDATE ci_items SET name = 'UpdatedByA' WHERE id = 1");

        $nameB = $pdoB->query('SELECT name FROM ci_items WHERE id = 1')->fetch(PDO::FETCH_ASSOC)['name'];
        $this->assertSame('Shared', $nameB);

        $nameA = $pdoA->query('SELECT name FROM ci_items WHERE id = 1')->fetch(PDO::FETCH_ASSOC)['name'];
        $this->assertSame('UpdatedByA', $nameA);
    }
}
