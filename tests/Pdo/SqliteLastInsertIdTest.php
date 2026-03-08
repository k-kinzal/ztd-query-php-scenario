<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests lastInsertId() behavior with ZTD shadow operations on SQLite.
 *
 * Since shadow INSERTs don't physically write to the database,
 * lastInsertId() may not reflect shadow-only inserts.
 * @spec SPEC-4.1
 */
class SqliteLastInsertIdTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_lid_test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_lid_test'];
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
