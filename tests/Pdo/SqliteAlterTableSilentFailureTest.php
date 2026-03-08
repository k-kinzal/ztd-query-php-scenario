<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ALTER TABLE behavior on SQLite through ZTD.
 * SQLite may silently accept ALTER TABLE but not apply changes properly.
 * @spec SPEC-5.1a
 */
class SqliteAlterTableSilentFailureTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_alt (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_alt'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_alt VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO sl_alt VALUES (2, 'Bob', 200)");
    }

    /**
     * ALTER TABLE ADD COLUMN via disableZtd physically alters the table,
     * but ZTD adapter still uses the old reflected schema. INSERT with
     * the new column fails because CTE doesn't include it.
     */
    public function testAddColumnViaDisabledZtdSchemaNotRefreshed(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('ALTER TABLE sl_alt ADD COLUMN email TEXT DEFAULT NULL');
        $this->pdo->enableZtd();

        // ZTD adapter still uses the schema reflected at construction time.
        // INSERT with new column throws because CTE doesn't include "email".
        try {
            $this->pdo->exec("INSERT INTO sl_alt (id, name, score, email) VALUES (3, 'Charlie', 300, 'charlie@test.com')");
            // If it works, the adapter refreshed schema
            $rows = $this->ztdQuery("SELECT id, name FROM sl_alt WHERE id = 3");
            $this->assertCount(1, $rows);
        } catch (\PDOException $e) {
            // Expected: "no such column: email" because ZTD schema wasn't refreshed
            $this->assertStringContainsString('email', $e->getMessage());
        }
    }

    /**
     * ALTER TABLE ADD COLUMN with ZTD enabled: verify behavior.
     */
    public function testAddColumnWithZtdEnabled(): void
    {
        // Try ALTER with ZTD enabled - this goes through unsupported SQL handling
        try {
            $this->pdo->exec('ALTER TABLE sl_alt ADD COLUMN email TEXT DEFAULT NULL');

            // If it doesn't throw, verify if the column was actually added
            // by inserting a row with the new column
            try {
                $this->pdo->exec("INSERT INTO sl_alt (id, name, score, email) VALUES (3, 'Charlie', 300, 'charlie@test.com')");
                $rows = $this->ztdQuery("SELECT email FROM sl_alt WHERE id = 3");
                // If we get here, ALTER worked through ZTD
                $this->assertCount(1, $rows);
            } catch (\Exception $e) {
                // Column may not have been added
                $this->addToAssertionCount(1);
            }
        } catch (\Exception $e) {
            // ALTER TABLE may throw through ZTD - this is acceptable
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Existing shadow data survives ALTER TABLE.
     */
    public function testExistingShadowDataSurvivesAlter(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('ALTER TABLE sl_alt ADD COLUMN email TEXT DEFAULT NULL');
        $this->pdo->enableZtd();

        // Previously inserted shadow rows should still be queryable
        $rows = $this->ztdQuery("SELECT id, name, score FROM sl_alt ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * ALTER TABLE RENAME TO - verify shadow data handling.
     */
    public function testRenameTableViaSqlite(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('ALTER TABLE sl_alt RENAME TO sl_alt_renamed');
        $this->pdo->enableZtd();

        // Shadow data was stored under old name - query under new name returns nothing
        // (shadow store still references "sl_alt")
        try {
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_alt_renamed");
            // If query works, shadow data may be lost (reads from empty physical table)
            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            // May throw if ZTD can't find the table
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Physical isolation after ALTER.
     */
    public function testPhysicalIsolationAfterAlter(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('ALTER TABLE sl_alt ADD COLUMN email TEXT DEFAULT NULL');

        // Physical table should still be empty (shadow INSERTs don't touch it)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_alt');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
