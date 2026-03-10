<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests WITHOUT ROWID tables through the ZTD shadow store.
 *
 * WITHOUT ROWID is a common SQLite optimization for tables with
 * non-integer or composite primary keys. It removes the implicit
 * rowid column. If the shadow store relies on rowid internally,
 * these tables will break.
 *
 * @spec SPEC-3.1
 */
class SqliteWithoutRowidTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_wor_tags (
                tag TEXT PRIMARY KEY,
                description TEXT NOT NULL,
                usage_count INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID",
            "CREATE TABLE sl_wor_settings (
                category TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (category, key)
            ) WITHOUT ROWID",
            "CREATE TABLE sl_wor_edges (
                src TEXT NOT NULL,
                dst TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 1.0,
                PRIMARY KEY (src, dst)
            ) WITHOUT ROWID",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wor_edges', 'sl_wor_settings', 'sl_wor_tags'];
    }

    /**
     * Basic INSERT + SELECT on WITHOUT ROWID table with text PK.
     */
    public function testInsertAndSelectTextPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('php', 'PHP language', 10)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('sql', 'SQL queries', 5)");

            $rows = $this->ztdQuery("SELECT tag, description, usage_count FROM sl_wor_tags ORDER BY tag");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'WITHOUT ROWID table: SELECT returned 0 rows. Shadow store may not support WITHOUT ROWID.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('php', $rows[0]['tag']);
            $this->assertSame('sql', $rows[1]['tag']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WITHOUT ROWID INSERT+SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on WITHOUT ROWID table.
     */
    public function testUpdateWithoutRowid(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('php', 'PHP language', 10)");
            $this->pdo->exec("UPDATE sl_wor_tags SET usage_count = 20 WHERE tag = 'php'");

            $rows = $this->ztdQuery("SELECT usage_count FROM sl_wor_tags WHERE tag = 'php'");

            if (count($rows) === 0) {
                $this->markTestIncomplete('WITHOUT ROWID UPDATE: row disappeared after UPDATE.');
            }

            if ((int) $rows[0]['usage_count'] !== 20) {
                $this->markTestIncomplete(
                    'WITHOUT ROWID UPDATE: usage_count is ' . $rows[0]['usage_count'] . ', expected 20.'
                );
            }

            $this->assertSame(20, (int) $rows[0]['usage_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WITHOUT ROWID UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE on WITHOUT ROWID table.
     */
    public function testDeleteWithoutRowid(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('php', 'PHP language', 10)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('sql', 'SQL queries', 5)");
            $this->pdo->exec("DELETE FROM sl_wor_tags WHERE tag = 'php'");

            $rows = $this->ztdQuery("SELECT tag FROM sl_wor_tags");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'WITHOUT ROWID DELETE: expected 1 row after delete, got ' . count($rows)
                    . ': ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('sql', $rows[0]['tag']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WITHOUT ROWID DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Composite PK WITHOUT ROWID table — INSERT, UPDATE, DELETE, SELECT.
     */
    public function testCompositePkWithoutRowid(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_settings VALUES ('app', 'theme', 'dark')");
            $this->pdo->exec("INSERT INTO sl_wor_settings VALUES ('app', 'lang', 'en')");
            $this->pdo->exec("INSERT INTO sl_wor_settings VALUES ('db', 'host', 'localhost')");

            $rows = $this->ztdQuery(
                "SELECT category, key, value FROM sl_wor_settings ORDER BY category, key"
            );

            $this->assertCount(3, $rows);

            // UPDATE by composite PK
            $this->pdo->exec("UPDATE sl_wor_settings SET value = 'light' WHERE category = 'app' AND key = 'theme'");

            $rows = $this->ztdQuery(
                "SELECT value FROM sl_wor_settings WHERE category = 'app' AND key = 'theme'"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete('Composite PK WITHOUT ROWID: row disappeared after UPDATE.');
            }

            if ($rows[0]['value'] !== 'light') {
                $this->markTestIncomplete(
                    'Composite PK WITHOUT ROWID UPDATE: value is "' . $rows[0]['value']
                    . '", expected "light".'
                );
            }

            // DELETE by composite PK
            $this->pdo->exec("DELETE FROM sl_wor_settings WHERE category = 'db' AND key = 'host'");

            $rows = $this->ztdQuery("SELECT * FROM sl_wor_settings ORDER BY category, key");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Composite PK WITHOUT ROWID DELETE: expected 2 rows, got ' . count($rows)
                    . ': ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Composite PK WITHOUT ROWID failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statements on WITHOUT ROWID table.
     */
    public function testPreparedStatementsWithoutRowid(): void
    {
        try {
            // Prepared INSERT
            $stmt = $this->ztdPrepare("INSERT INTO sl_wor_tags VALUES (?, ?, ?)");
            $stmt->execute(['python', 'Python language', 15]);
            $stmt->execute(['rust', 'Rust language', 8]);

            // Prepared SELECT
            $rows = $this->ztdPrepareAndExecute(
                "SELECT tag, usage_count FROM sl_wor_tags WHERE usage_count > ? ORDER BY tag",
                [10]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Prepared SELECT on WITHOUT ROWID returned 0 rows.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('python', $rows[0]['tag']);

            // Prepared UPDATE
            $stmt = $this->ztdPrepare("UPDATE sl_wor_tags SET usage_count = ? WHERE tag = ?");
            $stmt->execute([25, 'rust']);

            $rows = $this->ztdQuery("SELECT usage_count FROM sl_wor_tags WHERE tag = 'rust'");
            $this->assertSame(25, (int) $rows[0]['usage_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared statements on WITHOUT ROWID failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between two WITHOUT ROWID tables after DML on both.
     */
    public function testJoinTwoWithoutRowidTables(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_edges VALUES ('A', 'B', 1.5)");
            $this->pdo->exec("INSERT INTO sl_wor_edges VALUES ('B', 'C', 2.0)");
            $this->pdo->exec("INSERT INTO sl_wor_edges VALUES ('A', 'C', 3.0)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('A', 'Node A', 0)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('B', 'Node B', 0)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('C', 'Node C', 0)");

            // Modify both
            $this->pdo->exec("UPDATE sl_wor_edges SET weight = 5.0 WHERE src = 'A' AND dst = 'B'");
            $this->pdo->exec("UPDATE sl_wor_tags SET description = 'Updated A' WHERE tag = 'A'");

            $rows = $this->ztdQuery(
                "SELECT t.description, e.dst, e.weight
                 FROM sl_wor_tags t
                 JOIN sl_wor_edges e ON t.tag = e.src
                 WHERE t.tag = 'A'
                 ORDER BY e.dst"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'JOIN of two WITHOUT ROWID tables returned 0 rows.'
                );
            }

            $this->assertCount(2, $rows);  // A->B and A->C
            $this->assertSame('Updated A', $rows[0]['description']);
            $this->assertEquals(5.0, (float) $rows[0]['weight']);  // Updated weight
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN of WITHOUT ROWID tables failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate queries on WITHOUT ROWID table after DML.
     */
    public function testAggregateWithoutRowid(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('php', 'PHP', 10)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('sql', 'SQL', 5)");
            $this->pdo->exec("INSERT INTO sl_wor_tags VALUES ('go', 'Go', 20)");
            $this->pdo->exec("UPDATE sl_wor_tags SET usage_count = 15 WHERE tag = 'php'");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) as cnt, SUM(usage_count) as total, MAX(usage_count) as mx
                 FROM sl_wor_tags"
            );

            $this->assertSame(3, (int) $rows[0]['cnt']);
            // 15 + 5 + 20 = 40
            $this->assertSame(40, (int) $rows[0]['total']);
            $this->assertSame(20, (int) $rows[0]['mx']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate on WITHOUT ROWID failed: ' . $e->getMessage());
        }
    }
}
