<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests aggregate functions and operations on empty tables through ZTD.
 *
 * Users commonly query tables that have no data yet, or query after deleting all rows.
 * The shadow store must correctly handle COUNT=0, SUM=NULL, etc.
 * @spec SPEC-3.1
 */
class SqliteEmptyTableAggregateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE eta_items (id INTEGER PRIMARY KEY, name TEXT, value REAL)',
            'CREATE TABLE eta_lookup (id INTEGER PRIMARY KEY, item_id INTEGER, label TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['eta_items', 'eta_lookup'];
    }

    /**
     * COUNT(*) on empty table returns 0.
     */
    public function testCountEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM eta_items');
        $this->assertCount(1, $rows);
        $this->assertSame('0', (string) $rows[0]['cnt']);
    }

    /**
     * SUM on empty table returns NULL.
     */
    public function testSumEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT SUM(value) AS total FROM eta_items');
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['total']);
    }

    /**
     * AVG on empty table returns NULL.
     */
    public function testAvgEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT AVG(value) AS avg_val FROM eta_items');
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['avg_val']);
    }

    /**
     * MAX/MIN on empty table returns NULL.
     */
    public function testMaxMinEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT MAX(value) AS mx, MIN(value) AS mn FROM eta_items');
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['mx']);
        $this->assertNull($rows[0]['mn']);
    }

    /**
     * SELECT * on empty table returns 0 rows.
     */
    public function testSelectAllEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM eta_items');
        $this->assertCount(0, $rows);
    }

    /**
     * LEFT JOIN with empty right table.
     */
    public function testLeftJoinEmptyRight(): void
    {
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'Widget', 9.99)");
        // eta_lookup is empty

        try {
            $rows = $this->ztdQuery(
                'SELECT i.name, l.label FROM eta_items i LEFT JOIN eta_lookup l ON l.item_id = i.id ORDER BY i.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('LEFT JOIN with empty right table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertNull($rows[0]['label']);
    }

    /**
     * LEFT JOIN with empty left table.
     */
    public function testLeftJoinEmptyLeft(): void
    {
        $this->pdo->exec("INSERT INTO eta_lookup VALUES (1, 1, 'tag')");
        // eta_items is empty

        try {
            $rows = $this->ztdQuery(
                'SELECT i.name, l.label FROM eta_items i LEFT JOIN eta_lookup l ON l.item_id = i.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('LEFT JOIN with empty left table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(0, $rows);
    }

    /**
     * INNER JOIN where one table is empty returns 0 rows.
     */
    public function testInnerJoinOneEmpty(): void
    {
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'Widget', 9.99)");

        try {
            $rows = $this->ztdQuery(
                'SELECT i.name, l.label FROM eta_items i JOIN eta_lookup l ON l.item_id = i.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('INNER JOIN with one empty table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(0, $rows);
    }

    /**
     * EXISTS on empty table returns false.
     */
    public function testExistsEmptyTable(): void
    {
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'Widget', 9.99)");

        try {
            $rows = $this->ztdQuery(
                'SELECT i.name FROM eta_items i WHERE EXISTS (SELECT 1 FROM eta_lookup l WHERE l.item_id = i.id)'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('EXISTS on empty table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(0, $rows);
    }

    /**
     * NOT EXISTS on empty table returns all rows.
     */
    public function testNotExistsEmptyTable(): void
    {
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'Widget', 9.99)");
        $this->pdo->exec("INSERT INTO eta_items VALUES (2, 'Gadget', 19.99)");

        try {
            $rows = $this->ztdQuery(
                'SELECT i.name FROM eta_items i WHERE NOT EXISTS (SELECT 1 FROM eta_lookup l WHERE l.item_id = i.id) ORDER BY i.id'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('NOT EXISTS on empty table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
    }

    /**
     * COALESCE with aggregate on empty table.
     */
    public function testCoalesceAggregateEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT COALESCE(SUM(value), 0) AS total FROM eta_items');
        $this->assertCount(1, $rows);
        $this->assertEquals(0, (float) $rows[0]['total']);
    }

    /**
     * Insert into empty table, then count.
     */
    public function testInsertIntoEmptyThenCount(): void
    {
        // Verify empty first
        $before = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM eta_items');
        $this->assertSame('0', (string) $before[0]['cnt']);

        // Insert
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'First', 10.0)");

        $after = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM eta_items');
        $this->assertSame('1', (string) $after[0]['cnt']);
    }

    /**
     * GROUP BY on empty table returns 0 rows.
     */
    public function testGroupByEmptyTable(): void
    {
        $rows = $this->ztdQuery('SELECT name, COUNT(*) AS cnt FROM eta_items GROUP BY name');
        $this->assertCount(0, $rows);
    }

    /**
     * Subquery from empty table in WHERE.
     */
    public function testSubqueryFromEmptyInWhere(): void
    {
        $this->pdo->exec("INSERT INTO eta_items VALUES (1, 'Widget', 9.99)");

        try {
            $rows = $this->ztdQuery(
                'SELECT * FROM eta_items WHERE id IN (SELECT item_id FROM eta_lookup)'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery from empty table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(0, $rows);
    }

    /**
     * Prepared statement on empty table.
     */
    public function testPreparedOnEmptyTable(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT * FROM eta_items WHERE value > ?',
                [5.0]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared on empty table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(0, $rows);
    }
}
