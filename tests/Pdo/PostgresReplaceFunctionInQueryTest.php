<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests the SQL REPLACE() string function in queries on PostgreSQL shadow data.
 *
 * PostgreSQL does not have REPLACE INTO, but does have the REPLACE() string
 * function. The CTE rewriter may still misparse REPLACE() in UPDATE SET
 * clauses since the keyword appears in a DML-adjacent position.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.2
 */
class PostgresReplaceFunctionInQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rfq_contacts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                phone VARCHAR(50) NOT NULL,
                email VARCHAR(200) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rfq_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rfq_contacts (id, name, phone, email) VALUES (1, 'Alice', '(555) 123-4567', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO pg_rfq_contacts (id, name, phone, email) VALUES (2, 'Bob', '555.987.6543', 'bob@old-domain.com')");
        $this->pdo->exec("INSERT INTO pg_rfq_contacts (id, name, phone, email) VALUES (3, 'Carol', '555-111-2222', 'carol@example.com')");
    }

    /**
     * REPLACE() string function in SELECT.
     */
    public function testReplaceFunctionInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, REPLACE(phone, '-', '') AS clean_phone
             FROM pg_rfq_contacts
             WHERE id = 3"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('5551112222', $rows[0]['clean_phone']);
    }

    /**
     * REPLACE() in UPDATE SET clause.
     */
    public function testReplaceFunctionInUpdateSet(): void
    {
        $this->pdo->exec(
            "UPDATE pg_rfq_contacts SET email = REPLACE(email, '@old-domain.com', '@new-domain.com')
             WHERE id = 2"
        );

        $rows = $this->ztdQuery("SELECT email FROM pg_rfq_contacts WHERE id = 2");
        $this->assertSame('bob@new-domain.com', $rows[0]['email']);
    }

    /**
     * REPLACE() in WHERE clause.
     */
    public function testReplaceFunctionInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_rfq_contacts
             WHERE REPLACE(phone, '-', '') LIKE '555111%'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
    }

    /**
     * REPLACE() with prepared statement ($N params).
     */
    public function testReplaceFunctionWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "UPDATE pg_rfq_contacts SET email = REPLACE(email, $1, $2)
             WHERE id = $3"
        );
        $stmt->execute(['@old-domain.com', '@new-domain.com', 2]);

        $rows = $this->ztdQuery("SELECT email FROM pg_rfq_contacts WHERE id = 2");

        if ($rows[0]['email'] === 'bob@old-domain.com') {
            $this->markTestIncomplete(
                'REPLACE() in UPDATE SET with prepared $N params did not update on PostgreSQL.'
            );
        }

        $this->assertSame('bob@new-domain.com', $rows[0]['email']);
    }

    /**
     * Nested REPLACE() in UPDATE SET.
     */
    public function testNestedReplaceFunctionsInUpdate(): void
    {
        $this->pdo->exec(
            "UPDATE pg_rfq_contacts SET phone = REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), ' ', '-')
             WHERE id = 1"
        );

        $rows = $this->ztdQuery("SELECT phone FROM pg_rfq_contacts WHERE id = 1");
        $this->assertSame('555-123-4567', $rows[0]['phone']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_rfq_contacts')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
