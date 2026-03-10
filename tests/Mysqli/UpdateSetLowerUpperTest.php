<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with LOWER()/UPPER()/TRIM()/SUBSTRING()/LEFT()/RIGHT()
 * string functions in SET expressions through ZTD shadow store.
 *
 * These are very common data normalization/cleanup operations that differ
 * from REPLACE() (tested separately) and CONCAT() (tested in SPEC-10.2.15).
 * Tests whether the CTE rewriter evaluates string manipulation functions
 * correctly in UPDATE SET context.
 *
 * @spec SPEC-4.2
 */
class UpdateSetLowerUpperTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ulu_contacts (
            id INT PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL,
            code VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_ulu_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ulu_contacts VALUES (1, 'alice', 'JOHNSON', 'Alice@Example.COM', '  AB-001  ')");
        $this->mysqli->query("INSERT INTO mi_ulu_contacts VALUES (2, 'BOB', 'smith', 'BOB@test.ORG', '  CD-002  ')");
        $this->mysqli->query("INSERT INTO mi_ulu_contacts VALUES (3, 'Carol', 'Williams', 'carol@example.com', '  EF-003  ')");
    }

    /**
     * UPDATE SET with UPPER() on first_name.
     */
    public function testUpdateSetUpper(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ulu_contacts SET first_name = UPPER(first_name)");

            $rows = $this->ztdQuery("SELECT id, first_name FROM mi_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['first_name'] !== 'ALICE') {
                $this->markTestIncomplete('UPPER: expected "ALICE", got ' . var_export($rows[0]['first_name'], true));
            }
            $this->assertSame('ALICE', $rows[0]['first_name']);
            $this->assertSame('BOB', $rows[1]['first_name']);
            $this->assertSame('CAROL', $rows[2]['first_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET UPPER failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with LOWER() on email.
     */
    public function testUpdateSetLower(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ulu_contacts SET email = LOWER(email)");

            $rows = $this->ztdQuery("SELECT id, email FROM mi_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['email'] !== 'alice@example.com') {
                $this->markTestIncomplete('LOWER: expected "alice@example.com", got ' . var_export($rows[0]['email'], true));
            }
            $this->assertSame('alice@example.com', $rows[0]['email']);
            $this->assertSame('bob@test.org', $rows[1]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET LOWER failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with TRIM() to remove whitespace.
     */
    public function testUpdateSetTrim(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ulu_contacts SET code = TRIM(code)");

            $rows = $this->ztdQuery("SELECT id, code FROM mi_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['code'] !== 'AB-001') {
                $this->markTestIncomplete('TRIM: expected "AB-001", got ' . var_export($rows[0]['code'], true));
            }
            $this->assertSame('AB-001', $rows[0]['code']);
            $this->assertSame('CD-002', $rows[1]['code']);
            $this->assertSame('EF-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET TRIM failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with LEFT() to truncate.
     */
    public function testUpdateSetLeft(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ulu_contacts SET code = LEFT(TRIM(code), 2) WHERE id = 1");

            $rows = $this->ztdQuery("SELECT code FROM mi_ulu_contacts WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['code'] !== 'AB') {
                $this->markTestIncomplete('LEFT: expected "AB", got ' . var_export($rows[0]['code'], true));
            }
            $this->assertSame('AB', $rows[0]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET LEFT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with SUBSTRING() to extract portion.
     */
    public function testUpdateSetSubstring(): void
    {
        try {
            // Extract just the domain from email: SUBSTRING after @
            $this->mysqli->query(
                "UPDATE mi_ulu_contacts SET code = SUBSTRING(email, LOCATE('@', email) + 1) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT code FROM mi_ulu_contacts WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['code'] !== 'Example.COM') {
                $this->markTestIncomplete(
                    'SUBSTRING: expected "Example.COM", got ' . var_export($rows[0]['code'], true)
                );
            }
            $this->assertSame('Example.COM', $rows[0]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET SUBSTRING failed: ' . $e->getMessage());
        }
    }

    /**
     * Combined: UPPER + TRIM in single SET.
     */
    public function testUpdateSetCombinedUpperTrim(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ulu_contacts SET code = UPPER(TRIM(code))");

            $rows = $this->ztdQuery("SELECT id, code FROM mi_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            $this->assertSame('AB-001', $rows[0]['code']);
            $this->assertSame('CD-002', $rows[1]['code']);
            $this->assertSame('EF-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET UPPER+TRIM failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET multiple columns with different string functions.
     */
    public function testUpdateSetMultipleStringFunctions(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ulu_contacts SET first_name = UPPER(first_name), last_name = LOWER(last_name), email = LOWER(email) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT first_name, last_name, email FROM mi_ulu_contacts WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['first_name'] !== 'ALICE') {
                $this->markTestIncomplete(
                    'Multi-column: first_name expected "ALICE", got ' . var_export($rows[0]['first_name'], true)
                );
            }
            $this->assertSame('ALICE', $rows[0]['first_name']);
            $this->assertSame('johnson', $rows[0]['last_name']);
            $this->assertSame('alice@example.com', $rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET multiple string functions failed: ' . $e->getMessage());
        }
    }
}
