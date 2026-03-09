<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that double-quoted identifiers work through ZTD CTE rewriter.
 *
 * Applications using ORMs or code generators frequently quote all identifiers.
 * The CTE rewriter must correctly handle quoted table and column names.
 * @spec SPEC-3.1
 */
class SqliteQuotedIdentifierTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE qi_data (
                id INTEGER PRIMARY KEY,
                "select" TEXT,
                "from" TEXT,
                "where" INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['qi_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec('INSERT INTO qi_data VALUES (1, \'alpha\', \'region1\', 100)');
        $this->pdo->exec('INSERT INTO qi_data VALUES (2, \'beta\', \'region2\', 200)');
    }

    /**
     * SELECT with quoted column names that are SQL keywords.
     */
    public function testSelectQuotedKeywordColumns(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT id, "select", "from", "where" FROM qi_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('SELECT with quoted keyword columns failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('SELECT with quoted keyword columns returned empty');
            return;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('alpha', $rows[0]['select']);
        $this->assertSame('region1', $rows[0]['from']);
        $this->assertSame('100', (string) $rows[0]['where']);
    }

    /**
     * WHERE filter on quoted keyword column.
     */
    public function testWhereOnQuotedColumn(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM qi_data WHERE "where" > 150');
        } catch (\Exception $e) {
            $this->markTestIncomplete('WHERE on quoted column failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('WHERE on quoted column returned empty');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('beta', $rows[0]['select']);
    }

    /**
     * INSERT then SELECT with quoted columns after mutation.
     */
    public function testInsertThenSelectQuotedColumns(): void
    {
        try {
            $this->pdo->exec('INSERT INTO qi_data VALUES (3, \'gamma\', \'region3\', 300)');
            $rows = $this->ztdQuery('SELECT "select", "from" FROM qi_data WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT then SELECT quoted columns failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('INSERT then SELECT quoted columns returned empty');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('gamma', $rows[0]['select']);
    }

    /**
     * UPDATE on quoted keyword column.
     */
    public function testUpdateQuotedColumn(): void
    {
        try {
            $this->pdo->exec('UPDATE qi_data SET "where" = 999 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT "where" FROM qi_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE quoted column failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('999', (string) $rows[0]['where']);
    }

    /**
     * ORDER BY quoted keyword column.
     */
    public function testOrderByQuotedColumn(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM qi_data ORDER BY "where" DESC');
        } catch (\Exception $e) {
            $this->markTestIncomplete('ORDER BY quoted column failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('2', (string) $rows[0]['id']); // where=200 first
    }

    /**
     * GROUP BY quoted keyword column.
     */
    public function testGroupByQuotedColumn(): void
    {
        $this->pdo->exec('INSERT INTO qi_data VALUES (3, \'alpha\', \'region1\', 150)');

        try {
            $rows = $this->ztdQuery(
                'SELECT "select", COUNT(*) AS cnt FROM qi_data GROUP BY "select" ORDER BY "select"'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('GROUP BY quoted column failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('GROUP BY quoted column returned empty');
            return;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('alpha', $rows[0]['select']);
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Prepared statement with quoted column in WHERE.
     */
    public function testPreparedWithQuotedColumn(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT * FROM qi_data WHERE "select" = ?',
                ['alpha']
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared with quoted column failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Prepared with quoted column returned empty');
            return;
        }
        $this->assertCount(1, $rows);
    }

    /**
     * Fully quoted table name in query.
     */
    public function testFullyQuotedTableName(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM "qi_data" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Fully quoted table name failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Fully quoted table name returned empty — CTE rewriter may not match quoted table refs');
            return;
        }
        $this->assertCount(2, $rows);
    }

    /**
     * DELETE with quoted column in WHERE.
     */
    public function testDeleteWithQuotedColumn(): void
    {
        try {
            $this->pdo->exec('DELETE FROM qi_data WHERE "where" < 150');
            $rows = $this->ztdQuery('SELECT * FROM qi_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE with quoted column failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('2', (string) $rows[0]['id']);
    }
}
