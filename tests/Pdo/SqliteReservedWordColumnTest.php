<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests column and table names that are SQL reserved words through
 * SQLite CTE shadow store.
 *
 * Column names like "order", "group", "select", "from", "where" are
 * commonly encountered in legacy schemas. The CTE rewriter must handle
 * quoted identifiers correctly when generating CTE definitions.
 *
 * @see SPEC-11.CHECK-COLUMN-NAME (related: "check" substring in column names)
 */
class SqliteReservedWordColumnTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rwc_items (
                id INTEGER PRIMARY KEY,
                "order" INTEGER NOT NULL,
                "group" TEXT NOT NULL,
                "select" TEXT,
                "value" REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rwc_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('INSERT INTO sl_rwc_items VALUES (1, 10, \'A\', \'first\', 100.0)');
        $this->pdo->exec('INSERT INTO sl_rwc_items VALUES (2, 20, \'A\', \'second\', 200.0)');
        $this->pdo->exec('INSERT INTO sl_rwc_items VALUES (3, 10, \'B\', NULL, 150.0)');
        $this->pdo->exec('INSERT INTO sl_rwc_items VALUES (4, 30, \'B\', \'fourth\', 50.0)');
    }

    /**
     * SELECT with quoted reserved-word columns.
     */
    public function testSelectReservedWordColumns(): void
    {
        $rows = $this->ztdQuery(
            'SELECT id, "order", "group", "select" FROM sl_rwc_items ORDER BY id'
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(10, $rows[0]['order']);
        $this->assertEquals('A', $rows[0]['group']);
        $this->assertEquals('first', $rows[0]['select']);
    }

    /**
     * WHERE clause using reserved-word column.
     */
    public function testWhereWithReservedWordColumn(): void
    {
        $rows = $this->ztdQuery(
            'SELECT id FROM sl_rwc_items WHERE "order" = 10 ORDER BY id'
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, $rows[0]['id']);
        $this->assertEquals(3, $rows[1]['id']);
    }

    /**
     * GROUP BY with reserved-word column name.
     */
    public function testGroupByReservedWord(): void
    {
        $rows = $this->ztdQuery(
            'SELECT "group", SUM("value") AS total
             FROM sl_rwc_items
             GROUP BY "group"
             ORDER BY "group"'
        );

        $this->assertCount(2, $rows);
        $this->assertEquals('A', $rows[0]['group']);
        $this->assertEquals(300, (float) $rows[0]['total']);
        $this->assertEquals('B', $rows[1]['group']);
        $this->assertEquals(200, (float) $rows[1]['total']);
    }

    /**
     * ORDER BY with reserved-word column name.
     */
    public function testOrderByReservedWord(): void
    {
        $rows = $this->ztdQuery(
            'SELECT id, "order" FROM sl_rwc_items ORDER BY "order" DESC'
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(30, $rows[0]['order']);
    }

    /**
     * UPDATE SET on reserved-word column.
     */
    public function testUpdateReservedWordColumn(): void
    {
        $this->pdo->exec('UPDATE sl_rwc_items SET "order" = 99 WHERE id = 1');

        $rows = $this->ztdQuery('SELECT "order" FROM sl_rwc_items WHERE id = 1');
        $this->assertEquals(99, $rows[0]['order']);
    }

    /**
     * Prepared statement with reserved-word column.
     */
    public function testPreparedWithReservedWord(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT id, "group" FROM sl_rwc_items WHERE "order" >= ? ORDER BY id',
            [20]
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, $rows[0]['id']);
        $this->assertEquals(4, $rows[1]['id']);
    }

    /**
     * INSERT with explicit reserved-word column list.
     */
    public function testInsertWithReservedWordColumns(): void
    {
        $this->pdo->exec(
            'INSERT INTO sl_rwc_items (id, "order", "group", "select", "value")
             VALUES (5, 40, \'C\', \'fifth\', 500.0)'
        );

        $rows = $this->ztdQuery('SELECT * FROM sl_rwc_items WHERE id = 5');
        $this->assertCount(1, $rows);
        $this->assertEquals(40, $rows[0]['order']);
        $this->assertEquals('C', $rows[0]['group']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_rwc_items')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
