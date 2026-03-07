<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests NULL handling edge cases on MySQLi: UPDATE SET NULL, IS NULL after mutation,
 * COALESCE chains, NULL in CASE, and prepared statements with NULL.
 */
class NullHandlingEdgeCasesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_nhe_contacts');
        $raw->query('CREATE TABLE mi_nhe_contacts (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), notes TEXT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_nhe_contacts VALUES (1, 'Alice', 'alice@test.com', '555-0001', 'VIP customer')");
        $this->mysqli->query("INSERT INTO mi_nhe_contacts VALUES (2, 'Bob', 'bob@test.com', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_nhe_contacts VALUES (3, 'Charlie', NULL, '555-0003', 'New lead')");
    }

    public function testUpdateSetToNull(): void
    {
        $this->mysqli->query("UPDATE mi_nhe_contacts SET email = NULL WHERE id = 1");

        $result = $this->mysqli->query("SELECT email FROM mi_nhe_contacts WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertNull($row['email']);
    }

    public function testIsNullAfterUpdateToNull(): void
    {
        $this->mysqli->query("UPDATE mi_nhe_contacts SET phone = NULL WHERE id = 1");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_nhe_contacts WHERE phone IS NULL");
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testIsNotNullAfterUpdateFromNull(): void
    {
        $this->mysqli->query("UPDATE mi_nhe_contacts SET phone = '555-9999' WHERE id = 2");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_nhe_contacts WHERE phone IS NOT NULL");
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testCoalesceChain(): void
    {
        $result = $this->mysqli->query("SELECT name, COALESCE(email, phone, 'no contact') AS contact FROM mi_nhe_contacts ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('alice@test.com', $rows[0]['contact']);
        $this->assertSame('bob@test.com', $rows[1]['contact']);
        $this->assertSame('555-0003', $rows[2]['contact']);
    }

    public function testNullInCaseExpression(): void
    {
        $result = $this->mysqli->query("
            SELECT name,
                   CASE
                       WHEN email IS NOT NULL AND phone IS NOT NULL THEN 'full'
                       WHEN email IS NOT NULL OR phone IS NOT NULL THEN 'partial'
                       ELSE 'none'
                   END AS contact_status
            FROM mi_nhe_contacts
            ORDER BY id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('full', $rows[0]['contact_status']);
        $this->assertSame('partial', $rows[1]['contact_status']);
        $this->assertSame('partial', $rows[2]['contact_status']);
    }

    public function testPreparedInsertWithNull(): void
    {
        $stmt = $this->mysqli->prepare("INSERT INTO mi_nhe_contacts (id, name, email, phone, notes) VALUES (?, ?, ?, ?, ?)");
        $id = 4;
        $name = 'Diana';
        $email = null;
        $phone = null;
        $notes = 'test';
        $stmt->bind_param('issss', $id, $name, $email, $phone, $notes);
        $stmt->execute();

        $result = $this->mysqli->query("SELECT email, phone, notes FROM mi_nhe_contacts WHERE id = 4");
        $row = $result->fetch_assoc();
        $this->assertNull($row['email']);
        $this->assertNull($row['phone']);
        $this->assertSame('test', $row['notes']);
    }

    public function testIfnullFunction(): void
    {
        $result = $this->mysqli->query("SELECT name, IFNULL(phone, 'N/A') AS phone_display FROM mi_nhe_contacts ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('555-0001', $rows[0]['phone_display']);
        $this->assertSame('N/A', $rows[1]['phone_display']);
        $this->assertSame('555-0003', $rows[2]['phone_display']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_nhe_contacts');
        $raw->close();
    }
}
