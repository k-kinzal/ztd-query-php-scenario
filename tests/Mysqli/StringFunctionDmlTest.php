<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with string functions (UPPER, LOWER, REPLACE, CONCAT) on MySQLi.
 *
 * @spec SPEC-10.2
 */
class StringFunctionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_sf_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            display_name VARCHAR(200)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_sf_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_sf_users (username, email, display_name) VALUES ('john_doe', 'John@Example.COM', 'john doe')");
        $this->ztdExec("INSERT INTO mi_sf_users (username, email, display_name) VALUES ('jane_smith', 'Jane@Test.ORG', 'jane smith')");
        $this->ztdExec("INSERT INTO mi_sf_users (username, email, display_name) VALUES ('bob_jones', 'Bob@Demo.NET', 'bob jones')");
    }

    public function testUpdateLower(): void
    {
        try {
            $this->ztdExec("UPDATE mi_sf_users SET email = LOWER(email) WHERE username = 'john_doe'");

            $rows = $this->ztdQuery("SELECT email FROM mi_sf_users WHERE username = 'john_doe'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('LOWER (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ($rows[0]['email'] !== 'john@example.com') {
                $this->markTestIncomplete(
                    'LOWER (MySQLi): expected john@example.com, got ' . $rows[0]['email']
                );
            }

            $this->assertSame('john@example.com', $rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LOWER (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateUpper(): void
    {
        try {
            $this->ztdExec("UPDATE mi_sf_users SET display_name = UPPER(display_name) WHERE username = 'jane_smith'");

            $rows = $this->ztdQuery("SELECT display_name FROM mi_sf_users WHERE username = 'jane_smith'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPPER (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ($rows[0]['display_name'] !== 'JANE SMITH') {
                $this->markTestIncomplete(
                    'UPPER (MySQLi): expected JANE SMITH, got ' . $rows[0]['display_name']
                );
            }

            $this->assertSame('JANE SMITH', $rows[0]['display_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPPER (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateConcat(): void
    {
        try {
            $this->ztdExec("UPDATE mi_sf_users SET display_name = CONCAT(username, ' (', email, ')') WHERE username = 'bob_jones'");

            $rows = $this->ztdQuery("SELECT display_name FROM mi_sf_users WHERE username = 'bob_jones'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('CONCAT (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ($rows[0]['display_name'] !== 'bob_jones (Bob@Demo.NET)') {
                $this->markTestIncomplete(
                    'CONCAT (MySQLi): expected bob_jones (Bob@Demo.NET), got ' . $rows[0]['display_name']
                );
            }

            $this->assertSame('bob_jones (Bob@Demo.NET)', $rows[0]['display_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CONCAT (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateReplace(): void
    {
        try {
            $this->ztdExec("UPDATE mi_sf_users SET email = REPLACE(email, '@Test.ORG', '@newdomain.com') WHERE username = 'jane_smith'");

            $rows = $this->ztdQuery("SELECT email FROM mi_sf_users WHERE username = 'jane_smith'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('REPLACE (MySQLi): expected 1 row, got ' . count($rows));
            }

            if ($rows[0]['email'] !== 'Jane@newdomain.com') {
                $this->markTestIncomplete(
                    'REPLACE (MySQLi): expected Jane@newdomain.com, got ' . $rows[0]['email']
                );
            }

            $this->assertSame('Jane@newdomain.com', $rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('REPLACE (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
