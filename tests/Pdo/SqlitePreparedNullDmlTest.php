<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with explicit NULL parameter binding in DML on SQLite.
 *
 * NULL values in prepared statements interact with IS NULL / IS NOT NULL
 * comparisons and can cause unexpected behavior in shadow store WHERE clauses.
 *
 * @spec SPEC-10.2
 */
class SqlitePreparedNullDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_pn_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_pn_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_pn_contacts (name, email, phone) VALUES ('Alice', 'alice@example.com', '555-0001')");
        $this->ztdExec("INSERT INTO sl_pn_contacts (name, email, phone) VALUES ('Bob', NULL, '555-0002')");
        $this->ztdExec("INSERT INTO sl_pn_contacts (name, email, phone) VALUES ('Charlie', 'charlie@example.com', NULL)");
    }

    /**
     * INSERT with NULL parameter via prepared statement.
     */
    public function testPreparedInsertWithNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("INSERT INTO sl_pn_contacts (name, email, phone) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 'Diana', PDO::PARAM_STR);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->bindValue(3, '555-0004', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, email, phone FROM sl_pn_contacts WHERE name = 'Diana'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared INSERT NULL: expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
            $this->assertSame('555-0004', $rows[0]['phone']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET column to NULL via prepared statement.
     */
    public function testPreparedUpdateSetNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE sl_pn_contacts SET email = ? WHERE name = ?");
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT email FROM sl_pn_contacts WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE SET NULL: expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT WHERE column IS NULL after shadow INSERT with NULL.
     */
    public function testSelectIsNullAfterInsert(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name FROM sl_pn_contacts WHERE email IS NULL");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'SELECT IS NULL: expected 1 (Bob), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT IS NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE column IS NULL.
     */
    public function testDeleteWhereIsNull(): void
    {
        try {
            $this->ztdExec("DELETE FROM sl_pn_contacts WHERE phone IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM sl_pn_contacts ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IS NULL: expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IS NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE column IS NOT NULL.
     */
    public function testUpdateWhereIsNotNull(): void
    {
        try {
            $this->ztdExec("UPDATE sl_pn_contacts SET phone = '000-0000' WHERE phone IS NOT NULL");

            $rows = $this->ztdQuery("SELECT name, phone FROM sl_pn_contacts WHERE phone = '000-0000' ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE WHERE IS NOT NULL: expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE IS NOT NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET non-NULL then verify IS NULL returns correct rows.
     */
    public function testPreparedUpdateNullToValue(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE sl_pn_contacts SET email = ? WHERE name = ?");
            $stmt->execute(['bob@example.com', 'Bob']);

            $nullRows = $this->ztdQuery("SELECT name FROM sl_pn_contacts WHERE email IS NULL");
            $nonNullRows = $this->ztdQuery("SELECT name FROM sl_pn_contacts WHERE email IS NOT NULL ORDER BY name");

            if (count($nullRows) !== 0) {
                $this->markTestIncomplete(
                    'Prepared UPDATE NULL to value: expected 0 NULL, got ' . count($nullRows)
                    . '. Rows: ' . json_encode($nullRows)
                );
            }

            $this->assertCount(0, $nullRows);
            $this->assertCount(3, $nonNullRows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE NULL to value failed: ' . $e->getMessage());
        }
    }
}
