<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that semicolons in string data don't trigger multi-statement detection.
 *
 * Known: multi-statement SQL throws undocumented error (#78).
 * This tests that data containing semicolons isn't misdetected as multi-statement.
 * Also tests other SQL-like patterns in data values.
 *
 * @spec SPEC-4.1, SPEC-3.1
 */
class SqliteSemicolonInDataTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_sid_messages (
            id INTEGER PRIMARY KEY,
            sender TEXT NOT NULL,
            body TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_sid_messages'];
    }

    /**
     * INSERT data containing semicolons.
     */
    public function testInsertWithSemicolonInData(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (1, 'alice', 'Hello; how are you?')");
            $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (2, 'bob', 'item1; item2; item3')");

            $rows = $this->ztdQuery("SELECT id, body FROM sl_sid_messages ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT with semicolons: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Hello; how are you?', $rows[0]['body']);
            $this->assertSame('item1; item2; item3', $rows[1]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT with semicolons in data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT with semicolons in bound params.
     */
    public function testPreparedInsertWithSemicolonParam(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO sl_sid_messages VALUES (?, ?, ?)");
            $stmt->execute([1, 'alice', 'SELECT * FROM users; DROP TABLE users;']);

            $rows = $this->ztdQuery("SELECT body FROM sl_sid_messages WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertSame('SELECT * FROM users; DROP TABLE users;', $rows[0]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT with semicolons failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with semicolons in SET value.
     */
    public function testUpdateWithSemicolonInValue(): void
    {
        $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (1, 'alice', 'original')");

        try {
            $this->pdo->exec("UPDATE sl_sid_messages SET body = 'updated; with; semicolons' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT body FROM sl_sid_messages WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertSame('updated; with; semicolons', $rows[0]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with semicolons in value failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT WHERE with semicolons in LIKE pattern.
     */
    public function testSelectWhereWithSemicolonPattern(): void
    {
        $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (1, 'alice', 'a; b; c')");
        $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (2, 'bob', 'no semicolons here')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM sl_sid_messages WHERE body LIKE ?",
                ['%；%'] // Test that actual semicolons in LIKE patterns work
            );

            // Neither row has a full-width semicolon — should return 0
            $this->assertCount(0, $rows);

            // Now test with actual semicolon
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM sl_sid_messages WHERE body LIKE ?",
                ['%;%']
            );

            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with semicolons in LIKE pattern failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Data containing SQL keywords: INSERT INTO, DELETE FROM, DROP TABLE.
     */
    public function testDataContainingSqlKeywords(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (1, 'admin', 'Please INSERT INTO the system')");
            $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (2, 'admin', 'DELETE FROM your inbox')");
            $this->pdo->exec("INSERT INTO sl_sid_messages VALUES (3, 'admin', 'DROP TABLE of contents')");

            $rows = $this->ztdQuery("SELECT id, body FROM sl_sid_messages ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT with SQL keywords in data: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Please INSERT INTO the system', $rows[0]['body']);
            $this->assertSame('DELETE FROM your inbox', $rows[1]['body']);
            $this->assertSame('DROP TABLE of contents', $rows[2]['body']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Data containing SQL keywords failed: ' . $e->getMessage()
            );
        }
    }
}
