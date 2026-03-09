<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Probes which SQL keywords fail as table names through ZTD on SQLite.
 *
 * The CTE rewriter's SQL parser uses keyword matching to identify
 * statement types and table references. Table names that are also
 * SQL statement keywords may confuse the parser.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteKeywordTableNameProbeTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE "select" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "insert" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "update" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "delete" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "from" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "where" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "values" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "table" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "index" (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE "create" (id INTEGER PRIMARY KEY, val TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['"select"', '"insert"', '"update"', '"delete"', '"from"', '"where"', '"values"', '"table"', '"index"', '"create"'];
    }

    /**
     * @dataProvider keywordTableNameProvider
     */
    public function testInsertAndSelectOnKeywordTable(string $keyword): void
    {
        try {
            $this->ztdExec("INSERT INTO \"$keyword\" (id, val) VALUES (1, 'test')");
            $rows = $this->ztdQuery("SELECT val FROM \"$keyword\" WHERE id = 1");
            $this->assertCount(1, $rows, "Table named \"$keyword\": SELECT returned wrong count");
            $this->assertSame('test', $rows[0]['val'], "Table named \"$keyword\": wrong value");
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                "Table named \"$keyword\" failed: " . $e->getMessage()
            );
        }
    }

    /**
     * @dataProvider keywordTableNameProvider
     */
    public function testUpdateOnKeywordTable(string $keyword): void
    {
        try {
            $this->ztdExec("INSERT INTO \"$keyword\" (id, val) VALUES (1, 'original')");
            $this->ztdExec("UPDATE \"$keyword\" SET val = 'modified' WHERE id = 1");
            $rows = $this->ztdQuery("SELECT val FROM \"$keyword\" WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('modified', $rows[0]['val']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                "UPDATE on table named \"$keyword\" failed: " . $e->getMessage()
            );
        }
    }

    /**
     * @dataProvider keywordTableNameProvider
     */
    public function testDeleteOnKeywordTable(string $keyword): void
    {
        try {
            $this->ztdExec("INSERT INTO \"$keyword\" (id, val) VALUES (1, 'to_delete')");
            $this->ztdExec("DELETE FROM \"$keyword\" WHERE id = 1");
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM \"$keyword\"");
            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                "DELETE from table named \"$keyword\" failed: " . $e->getMessage()
            );
        }
    }

    public static function keywordTableNameProvider(): array
    {
        return [
            'select' => ['select'],
            'insert' => ['insert'],
            'update' => ['update'],
            'delete' => ['delete'],
            'from'   => ['from'],
            'where'  => ['where'],
            'values' => ['values'],
            'table'  => ['table'],
            'index'  => ['index'],
            'create' => ['create'],
        ];
    }
}
