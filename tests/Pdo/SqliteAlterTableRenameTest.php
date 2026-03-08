<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ALTER TABLE ... RENAME TO behavior on SQLite ZTD.
 *
 * The SqliteMutationResolver::resolveAlterRenameTable() creates a
 * DropTableMutation for the old name but does NOT create the table
 * under the new name in the shadow store.
 *
 * After rename:
 * - Old table name: shadow is dropped, queries fall through to physical DB
 * - New table name: NOT in shadow, queries fail (no physical table either)
 * - ZTD-inserted data is lost (shadow was the only place it existed)
 * @spec pending
 */
class SqliteAlterTableRenameTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE rename_src (id INTEGER PRIMARY KEY, name TEXT, score INT)';
    }

    protected function getTableNames(): array
    {
        return ['rename_src'];
    }


    /**
     * RENAME TABLE drops shadow for old name.
     * Since the shadow is dropped, queries fall through to physical DB,
     * returning only the original physical rows (not ZTD-inserted ones).
     */
    public function testRenameTableOldNameFallsThroughToPhysical(): void
    {
        // Insert in ZTD mode
        $this->pdo->exec("INSERT INTO rename_src (id, name, score) VALUES (3, 'Charlie', 70)");

        $this->pdo->exec('ALTER TABLE rename_src RENAME TO rename_dst');

        // Old name: shadow dropped, falls through to physical DB
        // Physical DB still has rename_src with 2 rows (SQLite :memory: rename didn't happen physically)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM rename_src');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    /**
     * After RENAME TABLE, the new table name is NOT accessible.
     * The resolver only drops the old name; it doesn't register the new name.
     */
    public function testRenameTableNewNameNotAccessible(): void
    {
        $this->pdo->exec('ALTER TABLE rename_src RENAME TO rename_dst');

        // New name doesn't exist in shadow or physical DB
        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM rename_dst');
    }

    /**
     * ZTD-inserted data is lost after rename because the shadow is dropped.
     */
    public function testZtdDataLostAfterRename(): void
    {
        $this->pdo->exec("INSERT INTO rename_src (id, name, score) VALUES (3, 'Charlie', 70)");

        // Before rename: ZTD has shadow data
        $stmt = $this->pdo->query("SELECT name FROM rename_src WHERE id = 3");
        $this->assertSame('Charlie', $stmt->fetchColumn());

        // Rename drops shadow
        $this->pdo->exec('ALTER TABLE rename_src RENAME TO rename_dst');

        // After rename: old name falls through to physical (which has no id=3)
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM rename_src WHERE id = 3");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation: rename is only in shadow layer.
     */
    public function testRenamePhysicalIsolation(): void
    {
        $this->pdo->exec('ALTER TABLE rename_src RENAME TO rename_dst');

        $this->pdo->disableZtd();

        // Physical table still exists under original name
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM rename_src');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
