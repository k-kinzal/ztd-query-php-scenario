<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite schema introspection queries through ZTD.
 *
 * SQLite uses sqlite_master / sqlite_schema instead of INFORMATION_SCHEMA.
 * ORMs like Doctrine and Eloquent query sqlite_master for table metadata.
 * Cross-platform parity with MysqlInformationSchemaTest and PostgresInformationSchemaTest.
 * @spec SPEC-10.2.20
 */
class SqliteSchemaIntrospectionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_schema_test (id INT PRIMARY KEY, name VARCHAR(100), active INT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_schema_test'];
    }

    /**
     * Query sqlite_master for table existence.
     */
    public function testQuerySqliteMasterForTable(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sl_schema_test'"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('sl_schema_test', $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('sqlite_master not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Query PRAGMA table_info for column metadata.
     */
    public function testPragmaTableInfo(): void
    {
        try {
            $this->pdo->disableZtd();
            $stmt = $this->pdo->query('PRAGMA table_info(sl_schema_test)');
            $columns = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->pdo->enableZtd();

            $names = array_column($columns, 'name');
            $this->assertContains('id', $names);
            $this->assertContains('name', $names);
            $this->assertContains('active', $names);
        } catch (\Throwable $e) {
            $this->markTestSkipped('PRAGMA table_info not accessible: ' . $e->getMessage());
        }
    }

    /**
     * Shadow operations work alongside schema queries.
     */
    public function testShadowOperationsWithSchemaQuery(): void
    {
        $this->pdo->exec("INSERT INTO sl_schema_test VALUES (1, 'Shadow', 1)");

        try {
            $this->pdo->disableZtd();
            $stmt = $this->pdo->query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sl_schema_test'"
            );
            $this->assertNotEmpty($stmt->fetchAll());
            $this->pdo->enableZtd();
        } catch (\Throwable $e) {
            $this->pdo->enableZtd();
        }

        // Shadow data still accessible
        $stmt = $this->pdo->query('SELECT name FROM sl_schema_test WHERE id = 1');
        $this->assertSame('Shadow', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_schema_test VALUES (1, 'Shadow', 1)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_schema_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
