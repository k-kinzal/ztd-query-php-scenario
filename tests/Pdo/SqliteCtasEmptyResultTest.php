<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TABLE AS SELECT (CTAS) behavior on SQLite.
 *
 * CTAS creates a table in the shadow registry, but on SQLite the newly
 * created table is not queryable (throws "no such table") because the
 * CTE rewriter cannot find it in the physical database.
 *
 * Also tests chained DDL → DML operations.
 * @spec SPEC-5.1
 */
class SqliteCtasEmptyResultTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ctas_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ctas_chain', 'ctas_source', 'ctas_copy', 'ctas_empty'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ctas_source VALUES (1, 'Alice', 95)");
        $this->pdo->exec("INSERT INTO ctas_source VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO ctas_source VALUES (3, 'Charlie', 75)");
    }
    /**
     * CTAS creates the table but querying it fails on SQLite
     * because the physical table doesn't exist.
     */
    public function testCtasTableNotQueryable(): void
    {
        // CTAS itself doesn't throw — it creates the shadow table
        $this->pdo->exec('CREATE TABLE ctas_copy AS SELECT * FROM ctas_source');

        // But querying the new table fails — "no such table"
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT * FROM ctas_copy');
    }

    /**
     * CTAS with empty result set (WHERE 1=0) — same behavior.
     */
    public function testCtasEmptyResultTableNotQueryable(): void
    {
        $this->pdo->exec('CREATE TABLE ctas_empty AS SELECT * FROM ctas_source WHERE 1=0');

        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT * FROM ctas_empty');
    }

    /**
     * Chained DDL → DML: DROP → CREATE → INSERT → SELECT works
     * because the physical table exists.
     */
    public function testDropCreateInsertCycle(): void
    {
        $this->pdo->exec('DROP TABLE ctas_source');
        $this->pdo->exec('CREATE TABLE ctas_source (id INT PRIMARY KEY, label VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ctas_source VALUES (1, 'Recreated')");

        $stmt = $this->pdo->query('SELECT label FROM ctas_source WHERE id = 1');
        $this->assertSame('Recreated', $stmt->fetchColumn());
    }

    /**
     * Chained DDL → DML with UPDATE.
     */
    public function testChainedDdlDml(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS ctas_chain');
        $this->pdo->exec('CREATE TABLE ctas_chain (id INT PRIMARY KEY, value VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ctas_chain VALUES (1, 'Step1')");
        $this->pdo->exec("UPDATE ctas_chain SET value = 'Step2' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT value FROM ctas_chain WHERE id = 1');
        $this->assertSame('Step2', $stmt->fetchColumn());
    }

    /**
     * Source table data preserved after failed CTAS query.
     */
    public function testSourceDataPreserved(): void
    {
        $this->pdo->exec('CREATE TABLE ctas_copy AS SELECT * FROM ctas_source');

        // Source table still works
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ctas_source');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }
}
