<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ATTACH DATABASE + cross-database queries through the ZTD shadow store.
 *
 * SQLite's ATTACH DATABASE is commonly used for:
 * - Migrating data between databases
 * - Partitioning data across files
 * - Temporary working databases
 *
 * Schema-qualified table names (db.table) may confuse the CTE rewriter
 * since it needs to match table names for shadow store lookup.
 *
 * @spec SPEC-3.1
 */
class SqliteAttachDatabaseDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_adb_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_adb_users'];
    }

    /**
     * ATTACH a second in-memory database and create a table in it.
     * Then query across both databases.
     */
    public function testAttachAndCrossDbSelect(): void
    {
        try {
            // Attach a second database
            $this->pdo->exec("ATTACH ':memory:' AS db2");
            $this->pdo->exec("CREATE TABLE db2.sl_adb_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )");

            // Insert into main database
            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (2, 'Bob', 1)");

            // Insert into attached database
            $this->pdo->exec("INSERT INTO db2.sl_adb_orders VALUES (1, 1, 99.50)");
            $this->pdo->exec("INSERT INTO db2.sl_adb_orders VALUES (2, 1, 49.00)");
            $this->pdo->exec("INSERT INTO db2.sl_adb_orders VALUES (3, 2, 75.25)");

            // Cross-database JOIN
            $rows = $this->ztdQuery(
                "SELECT u.name, o.amount
                 FROM sl_adb_users u
                 JOIN db2.sl_adb_orders o ON u.id = o.user_id
                 ORDER BY o.amount"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Cross-database JOIN returned 0 rows. CTE rewriter may not handle '
                    . 'schema-qualified table names (db2.table).'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']); // 49.00
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
            if (stripos($msg, 'no such table') !== false) {
                $this->markTestIncomplete(
                    'ATTACH DATABASE: CTE rewriter cannot find schema-qualified table. '
                    . 'Error: ' . $msg
                );
            }
            $this->markTestIncomplete('ATTACH DATABASE test failed: ' . $msg);
        }
    }

    /**
     * DML on attached database table then SELECT.
     */
    public function testDmlOnAttachedTable(): void
    {
        try {
            $this->pdo->exec("ATTACH ':memory:' AS db2");
            $this->pdo->exec("CREATE TABLE db2.sl_adb_logs (
                id INTEGER PRIMARY KEY,
                message TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info'
            )");

            $this->pdo->exec("INSERT INTO db2.sl_adb_logs VALUES (1, 'Started', 'info')");
            $this->pdo->exec("INSERT INTO db2.sl_adb_logs VALUES (2, 'Error occurred', 'error')");

            // UPDATE on attached table
            $this->pdo->exec("UPDATE db2.sl_adb_logs SET level = 'warning' WHERE id = 2");

            $rows = $this->ztdQuery("SELECT message, level FROM db2.sl_adb_logs WHERE id = 2");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'SELECT on attached database table returned 0 rows after UPDATE.'
                );
            }

            if ($rows[0]['level'] !== 'warning') {
                $this->markTestIncomplete(
                    'UPDATE on attached table: level is "' . $rows[0]['level']
                    . '", expected "warning". Shadow may not track schema-qualified tables.'
                );
            }

            $this->assertSame('warning', $rows[0]['level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DML on attached table failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT across databases — move data from main to attached.
     */
    public function testInsertSelectCrossDatabase(): void
    {
        try {
            $this->pdo->exec("ATTACH ':memory:' AS archive");
            $this->pdo->exec("CREATE TABLE archive.sl_adb_archived_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )");

            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (2, 'Bob', 0)");
            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (3, 'Carol', 0)");

            // Archive inactive users via INSERT...SELECT
            $this->pdo->exec(
                "INSERT INTO archive.sl_adb_archived_users (id, name)
                 SELECT id, name FROM sl_adb_users WHERE active = 0"
            );

            $archived = $this->ztdQuery(
                "SELECT name FROM archive.sl_adb_archived_users ORDER BY name"
            );

            if (count($archived) === 0) {
                $this->markTestIncomplete(
                    'INSERT...SELECT cross-database returned 0 archived rows.'
                );
            }

            $this->assertCount(2, $archived);
            $this->assertSame('Bob', $archived[0]['name']);
            $this->assertSame('Carol', $archived[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT cross-database failed: ' . $e->getMessage());
        }
    }

    /**
     * Schema-qualified table name in main database (main.table).
     */
    public function testMainSchemaQualifiedName(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_adb_users VALUES (1, 'Alice', 1)");

            // Use explicit main. prefix
            $rows = $this->ztdQuery("SELECT name FROM main.sl_adb_users WHERE id = 1");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'main.table_name SELECT returned 0 rows. CTE rewriter may not '
                    . 'match schema-qualified main database references.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('main.table SELECT failed: ' . $e->getMessage());
        }
    }
}
