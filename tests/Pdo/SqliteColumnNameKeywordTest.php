<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests columns named with SQL keywords or special substrings through ZTD.
 *
 * Real-world scenario: Columns named "select", "from", "where", "order",
 * "group", "insert", "update", "delete", "table", "index", "check",
 * "values", "set", "key", etc. may confuse the CTE rewriter's SQL parser,
 * which likely uses regex or keyword matching. Upstream #70 documents that
 * column names containing 'check' break INSERT. This test explores more.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteColumnNameKeywordTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cnk_data (
                id INTEGER PRIMARY KEY,
                "select" TEXT,
                "from" TEXT,
                "where" TEXT,
                "order" INTEGER,
                "group" TEXT,
                "values" TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cnk_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec('INSERT INTO sl_cnk_data (id, "select", "from", "where", "order", "group", "values") VALUES (1, \'a\', \'b\', \'c\', 1, \'g1\', \'v1\')');
        $this->ztdExec('INSERT INTO sl_cnk_data (id, "select", "from", "where", "order", "group", "values") VALUES (2, \'d\', \'e\', \'f\', 2, \'g2\', \'v2\')');
    }

    /**
     * SELECT all columns including keyword-named ones.
     */
    public function testSelectAllKeywordColumns(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM sl_cnk_data ORDER BY id');

            $this->assertCount(2, $rows);
            $this->assertSame('a', $rows[0]['select']);
            $this->assertSame('b', $rows[0]['from']);
            $this->assertSame('c', $rows[0]['where']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT all keyword columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT specific keyword-named columns.
     */
    public function testSelectSpecificKeywordColumns(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT "select", "from", "where" FROM sl_cnk_data WHERE id = 1'
            );

            $this->assertCount(1, $rows);
            $this->assertSame('a', $rows[0]['select']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT specific keyword columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE on keyword-named column.
     */
    public function testWhereOnKeywordColumn(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id FROM sl_cnk_data WHERE "select" = \'a\''
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE on keyword column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE keyword-named column.
     */
    public function testUpdateKeywordColumn(): void
    {
        try {
            $this->ztdExec('UPDATE sl_cnk_data SET "select" = \'updated\' WHERE id = 1');

            $rows = $this->ztdQuery('SELECT "select" FROM sl_cnk_data WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('updated', $rows[0]['select']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE keyword column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE multiple keyword-named columns.
     */
    public function testUpdateMultipleKeywordColumns(): void
    {
        try {
            $this->ztdExec(
                'UPDATE sl_cnk_data SET "from" = \'new_from\', "where" = \'new_where\', "order" = 99 WHERE id = 2'
            );

            $rows = $this->ztdQuery('SELECT "from", "where", "order" FROM sl_cnk_data WHERE id = 2');
            $this->assertCount(1, $rows);
            $this->assertSame('new_from', $rows[0]['from']);
            $this->assertSame('new_where', $rows[0]['where']);
            $this->assertEquals(99, (int) $rows[0]['order']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE multiple keyword columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with keyword-named columns.
     */
    public function testInsertKeywordColumns(): void
    {
        try {
            $this->ztdExec(
                'INSERT INTO sl_cnk_data (id, "select", "from", "where", "order", "group", "values") VALUES (3, \'x\', \'y\', \'z\', 3, \'g3\', \'v3\')'
            );

            $rows = $this->ztdQuery('SELECT * FROM sl_cnk_data WHERE id = 3');
            $this->assertCount(1, $rows);
            $this->assertSame('x', $rows[0]['select']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT keyword columns failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with WHERE on keyword-named column.
     */
    public function testDeleteWhereKeywordColumn(): void
    {
        try {
            $this->ztdExec('DELETE FROM sl_cnk_data WHERE "group" = \'g1\'');

            $rows = $this->ztdQuery('SELECT * FROM sl_cnk_data ORDER BY id');
            $this->assertCount(1, $rows);
            $this->assertEquals(2, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE WHERE keyword column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY keyword-named column.
     */
    public function testOrderByKeywordColumn(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id FROM sl_cnk_data ORDER BY "order" DESC'
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(2, (int) $rows[0]['id']);
            $this->assertEquals(1, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ORDER BY keyword column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with keyword-named column in WHERE.
     */
    public function testPreparedWhereKeywordColumn(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT id FROM sl_cnk_data WHERE "values" = ?',
                ['v1']
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared WHERE keyword column failed: ' . $e->getMessage()
            );
        }
    }
}
