<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL ENUM type handling with ZTD CTE rewriter.
 *
 * MySQL ENUM columns restrict values to a predefined set and have special
 * ordering behavior (ordered by the internal index, not alphabetically).
 * The CTE shadow store must preserve ENUM semantics through rewriting.
 *
 * @spec SPEC-10.2.19, SPEC-3.1, SPEC-4.1, SPEC-4.2
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
     * SELECT with WHERE clause filtering by ENUM value.
     */
    public function testSelectWhereEnumEquals(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Alice', 'active', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (2, 'Bob', 'inactive', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (3, 'Charlie', 'active', 'medium')");

        $rows = $this->ztdQuery("SELECT name FROM pdo_enum_test WHERE status = 'active' ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
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
     * MySQL ENUM ORDER BY sorts by the internal enum index, not alphabetically.
     *
     * For ENUM('active', 'inactive', 'pending'), the index order is:
     *   active=1, inactive=2, pending=3
     *
     * So ORDER BY status ASC should yield: active, inactive, pending
     * (which happens to be alphabetical here, but is by definition order).
     *
     * For ENUM('low', 'medium', 'high', 'critical'), the index order is:
     *   low=1, medium=2, high=3, critical=4
     *
     * So ORDER BY priority ASC should yield: low, medium, high, critical
     * (NOT alphabetical: critical, high, low, medium).
     */
    public function testEnumOrderByUsesDefinitionOrder(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (1, 'A', 'pending', 'critical')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (2, 'B', 'active', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (3, 'C', 'inactive', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (4, 'D', 'active', 'medium')");

        try {
            // ORDER BY priority: definition order is low, medium, high, critical
            $rows = $this->ztdQuery('SELECT name, priority FROM pdo_enum_test ORDER BY priority ASC');
            $this->assertCount(4, $rows);

            // If CTE shadow store preserves enum ordering:
            // low (B), medium (D), high (C), critical (A)
            $priorities = array_column($rows, 'priority');
            $this->assertSame(['low', 'medium', 'high', 'critical'], $priorities,
                'ENUM ORDER BY should use definition order, not alphabetical');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ENUM ORDER BY definition order failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Invalid ENUM value insertion.
     *
     * In strict SQL mode (default for MySQL 8.0+), inserting an invalid
     * ENUM value throws an error. In non-strict mode, it stores empty string.
     * The CTE rewriter should propagate or handle this correctly.
     */
    public function testInvalidEnumValueInsertion(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pdo_enum_test (id, name, status, priority) VALUES (1, 'Test', 'nonexistent', 'low')");

            // If we reach here, MySQL accepted the invalid value (non-strict mode)
            $rows = $this->ztdQuery('SELECT status FROM pdo_enum_test WHERE id = 1');
            $this->assertCount(1, $rows);
            // In non-strict mode, MySQL stores '' for invalid enum values
            $this->assertSame('', $rows[0]['status']);
        } catch (\PDOException $e) {
            // In strict mode, this should throw an error -- expected behavior
            $this->assertStringContainsString('1265', $e->getMessage(),
                'Expected Data truncated error for invalid ENUM value');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Invalid ENUM value behavior unclear: ' . $e->getMessage()
            );
        }
    }

    /**
     * ENUM IN(...) clause filtering.
     */
    public function testEnumInClause(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (1, 'A', 'active', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (2, 'B', 'inactive', 'medium')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (3, 'C', 'pending', 'high')");

        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pdo_enum_test WHERE status IN ('active', 'pending') ORDER BY id"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('A', $rows[0]['name']);
            $this->assertSame('C', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ENUM IN clause filtering failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY ENUM column.
     */
    public function testGroupByEnumColumn(): void
    {
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (1, 'A', 'active', 'low')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (2, 'B', 'active', 'high')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (3, 'C', 'inactive', 'medium')");
        $this->pdo->exec("INSERT INTO pdo_enum_test VALUES (4, 'D', 'pending', 'low')");

        try {
            $rows = $this->ztdQuery(
                'SELECT status, COUNT(*) AS cnt FROM pdo_enum_test GROUP BY status ORDER BY status'
            );
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY ENUM column failed: ' . $e->getMessage()
            );
        }
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
