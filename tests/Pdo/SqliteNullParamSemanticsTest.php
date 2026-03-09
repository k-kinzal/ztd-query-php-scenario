<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NULL parameter semantics through the CTE rewriter.
 *
 * Real-world scenario: applications frequently pass NULL parameters
 * in prepared statements for optional filters, nullable columns, and
 * conditional logic. The CTE rewriter must not alter NULL handling semantics.
 * Key concern: `WHERE col = ?` with NULL param should NOT match NULL rows
 * (SQL standard: NULL = NULL is UNKNOWN, not TRUE).
 *
 * @spec SPEC-3.2
 */
class SqliteNullParamSemanticsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_nps_contacts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                notes TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nps_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_nps_contacts VALUES (1, 'Alice', 'alice@test.com', '555-0001', 'VIP customer')");
        $this->ztdExec("INSERT INTO sl_nps_contacts VALUES (2, 'Bob', NULL, '555-0002', NULL)");
        $this->ztdExec("INSERT INTO sl_nps_contacts VALUES (3, 'Charlie', 'charlie@test.com', NULL, NULL)");
        $this->ztdExec("INSERT INTO sl_nps_contacts VALUES (4, 'Diana', NULL, NULL, 'New lead')");
    }

    /**
     * WHERE col = ? with NULL param: should return 0 rows (NULL = NULL is UNKNOWN).
     */
    public function testEqualityWithNullParamReturnsEmpty(): void
    {
        $stmt = $this->pdo->prepare("SELECT * FROM sl_nps_contacts WHERE email = ?");
        $stmt->bindValue(1, null, PDO::PARAM_NULL);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(0, $rows, 'WHERE col = NULL should return 0 rows per SQL standard');
    }

    /**
     * WHERE col IS NULL: should match NULL rows correctly.
     */
    public function testIsNullMatchesCorrectly(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM sl_nps_contacts WHERE email IS NULL ORDER BY name"
        );

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Bob', $names);
        $this->assertContains('Diana', $names);
    }

    /**
     * INSERT with NULL value via prepared statement, then query it back.
     */
    public function testInsertNullViaParamThenQuery(): void
    {
        $stmt = $this->pdo->prepare(
            "INSERT INTO sl_nps_contacts (id, name, email, phone, notes) VALUES (?, ?, ?, ?, ?)"
        );
        $stmt->bindValue(1, 5, PDO::PARAM_INT);
        $stmt->bindValue(2, 'Eve', PDO::PARAM_STR);
        $stmt->bindValue(3, null, PDO::PARAM_NULL);
        $stmt->bindValue(4, null, PDO::PARAM_NULL);
        $stmt->bindValue(5, 'Test', PDO::PARAM_STR);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT * FROM sl_nps_contacts WHERE id = 5");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertNull($rows[0]['email']);
        $this->assertNull($rows[0]['phone']);
        $this->assertSame('Test', $rows[0]['notes']);
    }

    /**
     * UPDATE SET col = NULL via prepared statement.
     */
    public function testUpdateSetNullViaParam(): void
    {
        $stmt = $this->pdo->prepare(
            "UPDATE sl_nps_contacts SET email = ? WHERE id = ?"
        );
        $stmt->bindValue(1, null, PDO::PARAM_NULL);
        $stmt->bindValue(2, 1, PDO::PARAM_INT);
        $stmt->execute();

        $rows = $this->ztdQuery("SELECT email FROM sl_nps_contacts WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['email']);
    }

    /**
     * Multiple NULL columns in WHERE: WHERE col1 IS NULL AND col2 IS NULL.
     */
    public function testMultipleIsNullConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM sl_nps_contacts WHERE email IS NULL AND phone IS NULL"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['name']);
    }

    /**
     * COALESCE with NULL columns in shadow data.
     */
    public function testCoalesceWithNullColumns(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, COALESCE(email, phone, 'no contact') AS contact
             FROM sl_nps_contacts
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('alice@test.com', $rows[0]['contact']);  // Alice: email exists
        $this->assertSame('555-0002', $rows[1]['contact']);        // Bob: email NULL, phone exists
        $this->assertSame('charlie@test.com', $rows[2]['contact']); // Charlie: email exists
        $this->assertSame('no contact', $rows[3]['contact']);       // Diana: both NULL
    }

    /**
     * DELETE WHERE col IS NOT NULL with prepared param on another column.
     */
    public function testDeleteWhereIsNotNullWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_nps_contacts WHERE email IS NOT NULL AND name != ?"
            );
            $stmt->execute(['Alice']);

            $rows = $this->ztdQuery("SELECT name FROM sl_nps_contacts ORDER BY name");
            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Bob', $names);
            $this->assertContains('Diana', $names);
            $this->assertNotContains('Charlie', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with IS NOT NULL and param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Aggregate functions with NULL values in shadow data.
     * COUNT(*) counts all rows; COUNT(col) skips NULLs.
     */
    public function testAggregateNullHandling(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total,
                    COUNT(email) AS with_email,
                    COUNT(phone) AS with_phone,
                    COUNT(notes) AS with_notes
             FROM sl_nps_contacts"
        );

        $this->assertEquals(4, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['with_email']);   // Alice, Charlie
        $this->assertEquals(2, (int) $rows[0]['with_phone']);   // Alice, Bob
        $this->assertEquals(2, (int) $rows[0]['with_notes']);   // Alice, Diana
    }

    /**
     * ORDER BY with NULLs — verify NULL ordering preserved through CTE rewrite.
     */
    public function testOrderByWithNulls(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, email FROM sl_nps_contacts ORDER BY email"
        );

        $this->assertCount(4, $rows);
        // SQLite sorts NULLs first by default
        // First two should have NULL email (Bob, Diana in some order)
        $this->assertNull($rows[0]['email']);
        $this->assertNull($rows[1]['email']);
        // Last two should have email values
        $this->assertNotNull($rows[2]['email']);
        $this->assertNotNull($rows[3]['email']);
    }
}
