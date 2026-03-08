<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests NULL handling edge cases on MySQL PDO: UPDATE SET NULL, IS NULL after mutation,
 * COALESCE chains, NULL in CASE, and prepared statements with NULL.
 * @spec SPEC-3.7
 */
class MysqlNullHandlingEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_nhe_contacts (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), notes TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['mysql_nhe_contacts'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_nhe_contacts VALUES (1, 'Alice', 'alice@test.com', '555-0001', 'VIP customer')");
        $this->pdo->exec("INSERT INTO mysql_nhe_contacts VALUES (2, 'Bob', 'bob@test.com', NULL, NULL)");
        $this->pdo->exec("INSERT INTO mysql_nhe_contacts VALUES (3, 'Charlie', NULL, '555-0003', 'New lead')");
    }

    public function testUpdateSetToNull(): void
    {
        $this->pdo->exec("UPDATE mysql_nhe_contacts SET email = NULL WHERE id = 1");

        $stmt = $this->pdo->query("SELECT email FROM mysql_nhe_contacts WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['email']);
    }

    public function testIsNullAfterUpdateToNull(): void
    {
        $this->pdo->exec("UPDATE mysql_nhe_contacts SET phone = NULL WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_nhe_contacts WHERE phone IS NULL");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testIsNotNullAfterUpdateFromNull(): void
    {
        $this->pdo->exec("UPDATE mysql_nhe_contacts SET phone = '555-9999' WHERE id = 2");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_nhe_contacts WHERE phone IS NOT NULL");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testCoalesceChain(): void
    {
        $stmt = $this->pdo->query("SELECT name, COALESCE(email, phone, 'no contact') AS contact FROM mysql_nhe_contacts ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('alice@test.com', $rows[0]['contact']);
        $this->assertSame('bob@test.com', $rows[1]['contact']);
        $this->assertSame('555-0003', $rows[2]['contact']);
    }

    public function testCoalesceWithAllNull(): void
    {
        $this->pdo->exec("INSERT INTO mysql_nhe_contacts VALUES (4, 'Diana', NULL, NULL, NULL)");

        $stmt = $this->pdo->query("SELECT COALESCE(email, phone, notes, 'no info') AS info FROM mysql_nhe_contacts WHERE id = 4");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('no info', $row['info']);
    }

    public function testNullInCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                   CASE
                       WHEN email IS NOT NULL AND phone IS NOT NULL THEN 'full'
                       WHEN email IS NOT NULL OR phone IS NOT NULL THEN 'partial'
                       ELSE 'none'
                   END AS contact_status
            FROM mysql_nhe_contacts
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('full', $rows[0]['contact_status']);
        $this->assertSame('partial', $rows[1]['contact_status']);
        $this->assertSame('partial', $rows[2]['contact_status']);
    }

    public function testPreparedUpdateToNull(): void
    {
        $stmt = $this->pdo->prepare("UPDATE mysql_nhe_contacts SET notes = ? WHERE id = ?");
        $stmt->execute([null, 1]);

        $select = $this->pdo->query("SELECT notes FROM mysql_nhe_contacts WHERE id = 1");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['notes']);
    }

    public function testPreparedInsertWithNull(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO mysql_nhe_contacts (id, name, email, phone, notes) VALUES (?, ?, ?, ?, ?)");
        $stmt->execute([4, 'Diana', null, null, 'test']);

        $select = $this->pdo->query("SELECT email, phone, notes FROM mysql_nhe_contacts WHERE id = 4");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['email']);
        $this->assertNull($row['phone']);
        $this->assertSame('test', $row['notes']);
    }

    public function testIfnullFunction(): void
    {
        $stmt = $this->pdo->query("SELECT name, IFNULL(phone, 'N/A') AS phone_display FROM mysql_nhe_contacts ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('555-0001', $rows[0]['phone_display']);
        $this->assertSame('N/A', $rows[1]['phone_display']);
        $this->assertSame('555-0003', $rows[2]['phone_display']);
    }
}
