<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NULL handling edge cases on PostgreSQL PDO: UPDATE SET NULL, IS NULL after mutation,
 * COALESCE chains, NULL in CASE, prepared statements with NULL, and NULLS FIRST/LAST ordering.
 * @spec pending
 */
class PostgresNullHandlingEdgeCasesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_nhe_contacts (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), notes TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_nhe_contacts'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_nhe_contacts VALUES (1, 'Alice', 'alice@test.com', '555-0001', 'VIP customer')");
        $this->pdo->exec("INSERT INTO pg_nhe_contacts VALUES (2, 'Bob', 'bob@test.com', NULL, NULL)");
        $this->pdo->exec("INSERT INTO pg_nhe_contacts VALUES (3, 'Charlie', NULL, '555-0003', 'New lead')");
    }

    public function testUpdateSetToNull(): void
    {
        $this->pdo->exec("UPDATE pg_nhe_contacts SET email = NULL WHERE id = 1");

        $stmt = $this->pdo->query("SELECT email FROM pg_nhe_contacts WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['email']);
    }

    public function testIsNullAfterUpdateToNull(): void
    {
        $this->pdo->exec("UPDATE pg_nhe_contacts SET phone = NULL WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_nhe_contacts WHERE phone IS NULL");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testIsNotNullAfterUpdateFromNull(): void
    {
        $this->pdo->exec("UPDATE pg_nhe_contacts SET phone = '555-9999' WHERE id = 2");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_nhe_contacts WHERE phone IS NOT NULL");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testCoalesceChain(): void
    {
        $stmt = $this->pdo->query("SELECT name, COALESCE(email, phone, 'no contact') AS contact FROM pg_nhe_contacts ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('alice@test.com', $rows[0]['contact']);
        $this->assertSame('bob@test.com', $rows[1]['contact']);
        $this->assertSame('555-0003', $rows[2]['contact']);
    }

    public function testCoalesceWithAllNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_nhe_contacts VALUES (4, 'Diana', NULL, NULL, NULL)");

        $stmt = $this->pdo->query("SELECT COALESCE(email, phone, notes, 'no info') AS info FROM pg_nhe_contacts WHERE id = 4");
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
            FROM pg_nhe_contacts
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('full', $rows[0]['contact_status']);
        $this->assertSame('partial', $rows[1]['contact_status']);
        $this->assertSame('partial', $rows[2]['contact_status']);
    }

    public function testPreparedUpdateToNull(): void
    {
        $stmt = $this->pdo->prepare("UPDATE pg_nhe_contacts SET notes = ? WHERE id = ?");
        $stmt->execute([null, 1]);

        $select = $this->pdo->query("SELECT notes FROM pg_nhe_contacts WHERE id = 1");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['notes']);
    }

    public function testPreparedInsertWithNull(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO pg_nhe_contacts (id, name, email, phone, notes) VALUES (?, ?, ?, ?, ?)");
        $stmt->execute([4, 'Diana', null, null, 'test']);

        $select = $this->pdo->query("SELECT email, phone, notes FROM pg_nhe_contacts WHERE id = 4");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['email']);
        $this->assertNull($row['phone']);
        $this->assertSame('test', $row['notes']);
    }

    public function testNullsFirstOrdering(): void
    {
        $stmt = $this->pdo->query("SELECT name, email FROM pg_nhe_contacts ORDER BY email NULLS FIRST");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['email']); // Charlie (NULL first)
    }

    public function testNullsLastOrdering(): void
    {
        $stmt = $this->pdo->query("SELECT name, email FROM pg_nhe_contacts ORDER BY email NULLS LAST");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertNull($rows[2]['email']); // Charlie (NULL last)
    }
}
