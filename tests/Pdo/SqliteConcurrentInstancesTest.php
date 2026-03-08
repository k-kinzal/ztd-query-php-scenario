<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that multiple ZtdPdo instances connected to the same physical database
 * maintain independent shadow stores with interleaved operations.
 * Shadow stores start empty — physical pre-existing data is NOT visible.
 * @spec SPEC-2.4
 */
class SqliteConcurrentInstancesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ci_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['ci_items'];
    }

    private string $dbPath;
    private ?ZtdPdo $pdoA = null;
    private ?ZtdPdo $pdoB = null;


    protected function setUp(): void
    {
        parent::setUp();

        $this->dbPath = tempnam(sys_get_temp_dir(), 'ztd_test_') . '.sqlite';
        // Create physical database with schema
        $setup = new PDO("sqlite:{$this->dbPath}", null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $setup->exec('CREATE TABLE ci_items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
        $setup = null;
        // Two separate ZtdPdo instances pointing at the same file
    }

    public function testBothInstancesStartWithEmptyShadow(): void
    {
        $stmtA = $this->pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        $stmtB = $this->pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testInsertInOneInstanceInvisibleToOther(): void
    {
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'FromA', 50)");

        // Instance A sees its own insert
        $stmtA = $this->pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(1, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        // Instance B still has empty shadow
        $stmtB = $this->pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testInterleavedInsertsBothInstancesIndependent(): void
    {
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'A-item1', 60)");
        $this->pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (2, 'B-item1', 70)");
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (3, 'A-item2', 80)");
        $this->pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (4, 'B-item2', 90)");

        // Instance A sees own inserts (1, 3) = 2
        $stmtA = $this->pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(2, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        // Instance B sees own inserts (2, 4) = 2
        $stmtB = $this->pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(2, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);

        // Verify data is distinct
        $namesA = $this->pdoA->query('SELECT name FROM ci_items ORDER BY name')->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['A-item1', 'A-item2'], $namesA);

        $namesB = $this->pdoB->query('SELECT name FROM ci_items ORDER BY name')->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['B-item1', 'B-item2'], $namesB);
    }

    public function testUpdateInOneInstanceInvisibleToOther(): void
    {
        // Both insert a row
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'Shared', 100)");
        $this->pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'Shared', 100)");

        // A updates its copy
        $this->pdoA->exec("UPDATE ci_items SET name = 'UpdatedByA' WHERE id = 1");

        // B still sees original
        $stmtB = $this->pdoB->query('SELECT name FROM ci_items WHERE id = 1');
        $this->assertSame('Shared', $stmtB->fetch(PDO::FETCH_ASSOC)['name']);

        // A sees updated
        $stmtA = $this->pdoA->query('SELECT name FROM ci_items WHERE id = 1');
        $this->assertSame('UpdatedByA', $stmtA->fetch(PDO::FETCH_ASSOC)['name']);
    }

    public function testDeleteInOneInstanceInvisibleToOther(): void
    {
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'ToDelete', 50)");
        $this->pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'ToDelete', 50)");

        $this->pdoA->exec("DELETE FROM ci_items WHERE id = 1");

        // B still sees the row
        $stmtB = $this->pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(1, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);

        // A sees 0 rows
        $stmtA = $this->pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testDisableZtdOnOneInstanceDoesNotAffectOther(): void
    {
        $this->pdoA->exec("INSERT INTO ci_items (id, name, score) VALUES (1, 'ShadowA', 50)");
        $this->pdoB->exec("INSERT INTO ci_items (id, name, score) VALUES (2, 'ShadowB', 60)");

        $this->pdoA->disableZtd();

        // A with ZTD disabled sees physical table (empty — shadow inserts were not persisted)
        $stmtA = $this->pdoA->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(0, (int) $stmtA->fetch(PDO::FETCH_ASSOC)['cnt']);

        // B still has its own independent ZTD session with its insert
        $stmtB = $this->pdoB->query('SELECT COUNT(*) AS cnt FROM ci_items');
        $this->assertSame(1, (int) $stmtB->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdoA->enableZtd();
    }

    protected function tearDown(): void
    {
        $this->pdoA = null;
        $this->pdoB = null;
        if (file_exists($this->dbPath)) {
            @unlink($this->dbPath);
        }
    }
}
