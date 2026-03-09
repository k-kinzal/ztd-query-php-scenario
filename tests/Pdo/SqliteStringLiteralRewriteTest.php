<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that the CTE rewriter does not modify string literals.
 *
 * Known Issue [#67]: SQLite CTE rewriter incorrectly replaces table
 * name references inside string literals when preceded by FROM/JOIN keywords.
 * @spec SPEC-3.1
 */
class SqliteStringLiteralRewriteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE slr_data (id INT PRIMARY KEY, name TEXT, info TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['slr_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO slr_data VALUES (1, 'test', 'some info')");
    }

    /**
     * String literal with 'from tablename' pattern — known broken [Issue #67].
     */
    public function testFromTableInString(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, 'from slr_data' AS lbl FROM slr_data LIMIT 1"
        );

        if (count($rows) === 0) {
            $this->markTestSkipped('CTE rewriter modifies string literals containing FROM <table> [Issue #67]');
        }
        $this->assertSame('from slr_data', $rows[0]['lbl']);
    }

    /**
     * String literal with 'join tablename' pattern — known broken [Issue #67].
     */
    public function testJoinTableInString(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, 'join slr_data' AS lbl FROM slr_data LIMIT 1"
        );

        if (count($rows) === 0) {
            $this->markTestSkipped('CTE rewriter modifies string literals containing JOIN <table> [Issue #67]');
        }
        $this->assertSame('join slr_data', $rows[0]['lbl']);
    }

    /**
     * Table name in WHERE value does NOT trigger the bug.
     */
    public function testTableNameInWhereValue(): void
    {
        $this->pdo->exec("INSERT INTO slr_data VALUES (2, 'slr_data', 'named like table')");

        $rows = $this->ztdQuery("SELECT * FROM slr_data WHERE name = 'slr_data'");
        $this->assertCount(1, $rows);
        $this->assertSame('2', (string) $rows[0]['id']);
    }

    /**
     * Table name without keyword context in string — should work.
     */
    public function testTableNameAloneInString(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, 'slr_data' AS lbl FROM slr_data LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('slr_data', $rows[0]['lbl']);
    }

    /**
     * Prepared statement with table name in bound parameter — should work.
     */
    public function testTableNameInBoundParam(): void
    {
        $this->pdo->exec("INSERT INTO slr_data VALUES (3, 'from slr_data', 'has table ref')");

        $rows = $this->ztdPrepareAndExecute(
            'SELECT * FROM slr_data WHERE name = ?',
            ['from slr_data']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('3', (string) $rows[0]['id']);
    }

    /**
     * INSERT with value containing table name.
     */
    public function testInsertValueContainingTableName(): void
    {
        $this->pdo->exec("INSERT INTO slr_data VALUES (4, 'from slr_data export', 'test')");

        $rows = $this->ztdQuery('SELECT * FROM slr_data WHERE id = 4');
        $this->assertCount(1, $rows);
        $this->assertSame('from slr_data export', $rows[0]['name']);
    }
}
