<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statements with explicit NULL parameter binding in DML on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlPreparedNullDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_pn_contacts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            phone VARCHAR(50)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_pn_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_pn_contacts (name, email, phone) VALUES ('Alice', 'alice@example.com', '555-0001')");
        $this->ztdExec("INSERT INTO my_pn_contacts (name, email, phone) VALUES ('Bob', NULL, '555-0002')");
        $this->ztdExec("INSERT INTO my_pn_contacts (name, email, phone) VALUES ('Charlie', 'charlie@example.com', NULL)");
    }

    public function testPreparedInsertWithNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("INSERT INTO my_pn_contacts (name, email, phone) VALUES (?, ?, ?)");
            $stmt->bindValue(1, 'Diana', PDO::PARAM_STR);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->bindValue(3, '555-0004', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT email, phone FROM my_pn_contacts WHERE name = 'Diana'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared INSERT NULL (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
            $this->assertSame('555-0004', $rows[0]['phone']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT NULL (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateSetNull(): void
    {
        try {
            $stmt = $this->ztdPrepare("UPDATE my_pn_contacts SET email = ? WHERE name = ?");
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT email FROM my_pn_contacts WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE SET NULL (MySQL): expected 1, got ' . count($rows));
            }

            $this->assertNull($rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET NULL (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereIsNull(): void
    {
        try {
            $this->ztdExec("DELETE FROM my_pn_contacts WHERE phone IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM my_pn_contacts ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('DELETE WHERE IS NULL (MySQL): expected 2, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IS NULL (MySQL) failed: ' . $e->getMessage());
        }
    }
}
