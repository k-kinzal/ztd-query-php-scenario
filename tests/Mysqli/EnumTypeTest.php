<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL ENUM column type via MySQLi.
 *
 * Cross-platform parity with MysqlEnumTypeTest (PDO).
 * @spec SPEC-10.2.19
 */
class EnumTypeTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_enum_test (
            id INT PRIMARY KEY,
            status ENUM(\'active\', \'inactive\', \'pending\'),
            priority ENUM(\'low\', \'medium\', \'high\', \'critical\')
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_enum_test'];
    }


    /**
     * INSERT with ENUM values.
     */
    public function testInsertEnumValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (1, 'active', 'high')");

        $result = $this->mysqli->query('SELECT status, priority FROM mi_enum_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('active', $row['status']);
        $this->assertSame('high', $row['priority']);
    }

    /**
     * UPDATE ENUM column.
     */
    public function testUpdateEnumColumn(): void
    {
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (1, 'active', 'low')");
        $this->mysqli->query("UPDATE mi_enum_test SET status = 'inactive' WHERE id = 1");

        $result = $this->mysqli->query('SELECT status FROM mi_enum_test WHERE id = 1');
        $this->assertSame('inactive', $result->fetch_assoc()['status']);
    }

    /**
     * WHERE comparison with ENUM.
     */
    public function testWhereWithEnumComparison(): void
    {
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (1, 'active', 'high')");
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (2, 'inactive', 'low')");
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (3, 'active', 'medium')");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_enum_test WHERE status = 'active'");
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * NULL ENUM value.
     */
    public function testNullEnumValue(): void
    {
        $this->mysqli->query('INSERT INTO mi_enum_test VALUES (1, NULL, NULL)');

        $result = $this->mysqli->query('SELECT status, priority FROM mi_enum_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertNull($row['status']);
        $this->assertNull($row['priority']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_enum_test VALUES (1, 'active', 'high')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_enum_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
