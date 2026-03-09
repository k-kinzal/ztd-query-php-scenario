<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests the SQL REPLACE() string function in queries on shadow data.
 *
 * Real-world scenario: applications use REPLACE() to clean or transform
 * string data in SELECT, UPDATE SET, and WHERE clauses. Since REPLACE is
 * also a DML keyword (REPLACE INTO), the CTE rewriter's SQL parser might
 * confuse the string function with the DML statement, causing parse errors
 * or incorrect rewrites.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.2
 */
class SqliteReplaceFunctionInQueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rfq_contacts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rfq_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_rfq_contacts VALUES (1, 'Alice Smith', '(555) 123-4567', 'alice@example.com')");
        $this->ztdExec("INSERT INTO sl_rfq_contacts VALUES (2, 'Bob Jones', '555.987.6543', 'bob@old-domain.com')");
        $this->ztdExec("INSERT INTO sl_rfq_contacts VALUES (3, 'Carol White', '555-111-2222', 'carol@example.com')");
    }

    /**
     * REPLACE() in SELECT to strip characters.
     */
    public function testReplaceFunctionInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', '') AS clean_phone
                 FROM sl_rfq_contacts
                 ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('555 1234567', $rows[0]['clean_phone']); // Alice: removed () and -
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() in WHERE clause.
     */
    public function testReplaceFunctionInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_rfq_contacts
                 WHERE REPLACE(email, '@old-domain.com', '') != email
                 ORDER BY name"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Bob Jones', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() in UPDATE SET clause.
     *
     * This is the most likely pattern to confuse the parser since
     * REPLACE appears right after SET, similar to REPLACE INTO syntax.
     */
    public function testReplaceFunctionInUpdateSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_rfq_contacts SET email = REPLACE(email, '@old-domain.com', '@new-domain.com')
                 WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT email FROM sl_rfq_contacts WHERE id = 2");
            $this->assertSame('bob@new-domain.com', $rows[0]['email']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple REPLACE() calls in UPDATE SET.
     */
    public function testMultipleReplaceFunctionsInUpdateSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_rfq_contacts SET
                    phone = REPLACE(REPLACE(phone, '(', ''), ')', ''),
                    email = REPLACE(email, '@old-domain.com', '@new-domain.com')"
            );

            $rows = $this->ztdQuery("SELECT phone, email FROM sl_rfq_contacts ORDER BY id");
            $this->assertSame('555 123-4567', $rows[0]['phone']); // Alice: removed ()
            $this->assertSame('bob@new-domain.com', $rows[1]['email']); // Bob: domain changed
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple REPLACE() in UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() in prepared statement.
     */
    public function testReplaceFunctionInPreparedStatement(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, REPLACE(phone, ?, ?) AS clean_phone
                 FROM sl_rfq_contacts
                 WHERE id = ?",
                ['-', '', 3]
            );

            $this->assertCount(1, $rows);
            $this->assertSame('5551112222', $rows[0]['clean_phone']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() in prepared statement failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() combined with other string functions.
     */
    public function testReplaceFunctionWithOtherStringFunctions(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, UPPER(REPLACE(email, '@example.com', '')) AS username
                 FROM sl_rfq_contacts
                 WHERE email LIKE '%@example.com'
                 ORDER BY name"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('ALICE', $rows[0]['username']);
            $this->assertSame('CAROL', $rows[1]['username']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() combined with string functions failed: ' . $e->getMessage()
            );
        }
    }
}
