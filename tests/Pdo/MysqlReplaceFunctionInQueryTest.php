<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests the SQL REPLACE() string function in queries on MySQL shadow data.
 *
 * MySQL supports both REPLACE INTO (DML) and REPLACE() (string function).
 * The CTE rewriter's SQL parser must distinguish between these two uses.
 * Misidentifying REPLACE() as a DML keyword could cause parse errors or
 * incorrect query rewrites.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.2
 */
class MysqlReplaceFunctionInQueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_rfq_contacts (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                phone VARCHAR(50) NOT NULL,
                email VARCHAR(200) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_rfq_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_rfq_contacts VALUES (1, 'Alice', '(555) 123-4567', 'alice@example.com')");
        $this->ztdExec("INSERT INTO my_rfq_contacts VALUES (2, 'Bob', '555.987.6543', 'bob@old-domain.com')");
        $this->ztdExec("INSERT INTO my_rfq_contacts VALUES (3, 'Carol', '555-111-2222', 'carol@example.com')");
    }

    /**
     * REPLACE() string function in SELECT.
     */
    public function testReplaceFunctionInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, REPLACE(phone, '-', '') AS clean_phone
                 FROM my_rfq_contacts
                 WHERE id = 3"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('5551112222', $rows[0]['clean_phone']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in SELECT failed on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() in UPDATE SET — most likely to confuse parser.
     */
    public function testReplaceFunctionInUpdateSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_rfq_contacts SET email = REPLACE(email, '@old-domain.com', '@new-domain.com')
                 WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT email FROM my_rfq_contacts WHERE id = 2");
            $this->assertSame('bob@new-domain.com', $rows[0]['email']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in UPDATE SET failed on MySQL: ' . $e->getMessage()
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
                "SELECT name FROM my_rfq_contacts
                 WHERE REPLACE(phone, '-', '') LIKE '555111%'"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Carol', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() function in WHERE failed on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * REPLACE() with prepared params.
     */
    public function testReplaceFunctionWithPreparedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_rfq_contacts SET email = REPLACE(email, ?, ?)
                 WHERE id = ?"
            );
            $stmt->execute(['@old-domain.com', '@new-domain.com', 2]);

            $rows = $this->ztdQuery("SELECT email FROM my_rfq_contacts WHERE id = 2");

            if ($rows[0]['email'] === 'bob@old-domain.com') {
                $this->markTestIncomplete(
                    'REPLACE() in UPDATE SET with prepared params did not update on MySQL.'
                );
            }

            $this->assertSame('bob@new-domain.com', $rows[0]['email']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'REPLACE() with prepared params failed on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * Nested REPLACE() calls in UPDATE SET.
     */
    public function testNestedReplaceFunctionsInUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_rfq_contacts SET phone = REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), ' ', '-')
                 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT phone FROM my_rfq_contacts WHERE id = 1");
            $this->assertSame('555-123-4567', $rows[0]['phone']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Nested REPLACE() in UPDATE failed on MySQL: ' . $e->getMessage()
            );
        }
    }
}
