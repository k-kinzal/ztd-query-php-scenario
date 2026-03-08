<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL ENUM type handling with ZTD.
 *
 * MySQL ENUM columns restrict values to a predefined set.
 * Tests whether the shadow store correctly handles ENUM values
 * through CTE rewriting.
 * @spec pending
 */
class MysqlEnumTypeTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_enum_test (id INT PRIMARY KEY, name VARCHAR(50), status ENUM(\'active\', \'inactive\', \'pending\'), priority ENUM(\'low\', \'medium\', \'high\', \'critical\'))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_enum_test'];
    }


    /**
     * INSERT with valid ENUM values.
     */
    public function testInsertWithValidEnumValues(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'high')");

        $stmt = $this->pdo->query('SELECT status, priority FROM pdo_enum_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('active', $row['status']);
        $this->assertSame('high', $row['priority']);
    }

    /**
     * INSERT with all ENUM variants.
     */
    public function testInsertAllEnumVariants(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'A', 'active', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (2, 'B', 'inactive', 'medium')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (3, 'C', 'pending', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (4, 'D', 'active', 'critical')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_enum_test');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE ENUM column.
     */
    public function testUpdateEnumColumn(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'low')");
        $this->pdo->exec("UPDATE pdo_enum_test SET status = 'inactive' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT status FROM pdo_enum_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('inactive', $row['status']);
    }

    /**
     * WHERE clause with ENUM comparison.
     */
    public function testWhereWithEnumComparison(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (2, 'Bob', 'inactive', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (3, 'Charlie', 'active', 'medium')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_enum_test WHERE status = 'active'");
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared statement with ENUM parameter.
     */
    public function testPreparedStatementWithEnumParam(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (2, 'Bob', 'inactive', 'low')");

        $stmt = $this->pdo->prepare("SELECT name FROM pdo_enum_test WHERE status = ?");
        $stmt->execute(['active']);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * INSERT with NULL ENUM value.
     */
    public function testNullEnumValue(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', NULL, NULL)");

        $stmt = $this->pdo->query('SELECT status, priority FROM pdo_enum_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['status']);
        $this->assertNull($row['priority']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'high')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_enum_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
