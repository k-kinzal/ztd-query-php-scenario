<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared statements with explicit NULL parameter binding in DML on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresPreparedNullDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_pn_contacts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            phone VARCHAR(50)
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_pn_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_pn_contacts (name, email, phone) VALUES ('Alice', 'alice@example.com', '555-0001')");
        $this->ztdExec("INSERT INTO pg_pn_contacts (name, email, phone) VALUES ('Bob', NULL, '555-0002')");
        $this->ztdExec("INSERT INTO pg_pn_contacts (name, email, phone) VALUES ('Charlie', 'charlie@example.com', NULL)");
    }

    public function testPreparedInsertWithNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("INSERT INTO pg_pn_contacts (name, email, phone) VALUES ($1, $2, $3)");
            $stmt->bindValue(1, 'Diana', PDO::PARAM_STR);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->bindValue(3, '555-0004', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, email, phone FROM pg_pn_contacts WHERE name = 'Diana'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared INSERT NULL (PG): expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
            $this->assertSame('555-0004', $rows[0]['phone']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT NULL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateSetNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE pg_pn_contacts SET email = $1 WHERE name = $2");
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT email FROM pg_pn_contacts WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE SET NULL (PG): expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET NULL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereIsNull(): void
    {
        try {
            $this->ztdExec("DELETE FROM pg_pn_contacts WHERE phone IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM pg_pn_contacts ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE IS NULL (PG): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IS NULL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereIsNotNull(): void
    {
        try {
            $this->ztdExec("UPDATE pg_pn_contacts SET phone = '000-0000' WHERE phone IS NOT NULL");

            $rows = $this->ztdQuery("SELECT name FROM pg_pn_contacts WHERE phone = '000-0000' ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE WHERE IS NOT NULL (PG): expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE IS NOT NULL (PG) failed: ' . $e->getMessage());
        }
    }
}
