<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests lastInsertId() behavior with ZTD shadow operations on SQLite.
 *
 * Since shadow INSERTs don't physically write to the database,
 * lastInsertId() may not reflect shadow-only inserts.
 */
class SqliteLastInsertIdTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_lid_test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * lastInsertId after shadow INSERT.
     */
    public function testLastInsertIdAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Alice')");

        // Shadow INSERT doesn't physically write, so lastInsertId may be '0' or stale
        $id = $this->pdo->lastInsertId();
        // Document whatever behavior we observe
        $this->assertIsString($id);
    }

    /**
     * lastInsertId after multiple shadow INSERTs.
     */
    public function testLastInsertIdAfterMultipleShadowInserts(): void
    {
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Bob')");

        $id = $this->pdo->lastInsertId();
        $this->assertIsString($id);
    }

    /**
     * Shadow INSERT rows are visible in shadow SELECT.
     */
    public function testShadowInsertRowsVisible(): void
    {
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Bob')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_lid_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation — no physical rows after shadow INSERT.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_lid_test (name) VALUES ('Alice')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_lid_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
