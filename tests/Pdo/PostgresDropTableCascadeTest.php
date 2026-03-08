<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DROP TABLE ... CASCADE on PostgreSQL ZTD.
 *
 * CASCADE/RESTRICT are PostgreSQL-specific modifiers for DROP TABLE.
 * The ZTD parser just extracts the table name; CASCADE/RESTRICT don't
 * affect shadow store behavior since there are no physical FK constraints.
 * @spec pending
 */
class PostgresDropTableCascadeTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dtc_parent (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE pg_dtc_child (id INT PRIMARY KEY, parent_id INT REFERENCES pg_dtc_parent(id))',
            'CREATE TABLE pg_dtc_temp (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE pg_dtc_restrict (id INT PRIMARY KEY, val TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dtc_child', 'pg_dtc_parent', 'nonexistent_cascade_table', 'pg_dtc_temp', 'pg_dtc_restrict'];
    }


    public function testDropTableCascadeOnShadowCreatedTable(): void
    {
        $this->pdo->exec('CREATE TABLE pg_dtc_temp (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_dtc_temp VALUES (1, 'hello')");

        $this->pdo->exec('DROP TABLE pg_dtc_temp CASCADE');

        // Table no longer exists in shadow
        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM pg_dtc_temp');
    }

    public function testDropTableCascadeOnReflectedTable(): void
    {
        $this->pdo->exec("INSERT INTO pg_dtc_parent VALUES (1, 'Alice')");

        $this->pdo->exec('DROP TABLE pg_dtc_parent CASCADE');

        // Table dropped from shadow, but physical table still exists
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dtc_parent');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public function testDropTableIfExistsCascade(): void
    {
        // Should not throw even if table doesn't exist in shadow
        $this->pdo->exec('DROP TABLE IF EXISTS nonexistent_cascade_table CASCADE');
        $this->assertTrue(true);
    }

    public function testDropTableRestrictOnShadowTable(): void
    {
        $this->pdo->exec('CREATE TABLE pg_dtc_restrict (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_dtc_restrict VALUES (1, 'hello')");

        // RESTRICT in shadow mode is effectively the same as CASCADE
        // since shadow store doesn't track FK dependencies
        $this->pdo->exec('DROP TABLE pg_dtc_restrict RESTRICT');

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM pg_dtc_restrict');
    }
}
