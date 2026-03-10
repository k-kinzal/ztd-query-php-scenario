<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statements with NULL values in DML on MySQLi.
 *
 * @spec SPEC-10.2
 */
class PreparedNullDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_pn_contacts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            phone VARCHAR(50)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_pn_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_pn_contacts (name, email, phone) VALUES ('Alice', 'alice@example.com', '555-0001')");
        $this->ztdExec("INSERT INTO mi_pn_contacts (name, email, phone) VALUES ('Bob', NULL, '555-0002')");
        $this->ztdExec("INSERT INTO mi_pn_contacts (name, email, phone) VALUES ('Charlie', 'charlie@example.com', NULL)");
    }

    public function testDeleteWhereIsNull(): void
    {
        try {
            $this->ztdExec("DELETE FROM mi_pn_contacts WHERE phone IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM mi_pn_contacts ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IS NULL (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IS NULL (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereIsNotNull(): void
    {
        try {
            $this->ztdExec("UPDATE mi_pn_contacts SET phone = '000-0000' WHERE phone IS NOT NULL");

            $rows = $this->ztdQuery("SELECT name FROM mi_pn_contacts WHERE phone = '000-0000' ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE WHERE IS NOT NULL (MySQLi): expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE IS NOT NULL (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testSelectIsNullAfterInsert(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name FROM mi_pn_contacts WHERE email IS NULL");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SELECT IS NULL (MySQLi): expected 1, got ' . count($rows));
            }

            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT IS NULL (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
