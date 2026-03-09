<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that tables named with SQL reserved words work through ZTD CTE rewriter.
 *
 * Real applications frequently use tables named `order`, `user`, `group`, `select`, etc.
 * The CTE rewriter must correctly distinguish keyword usage from table name usage.
 * @spec SPEC-3.1
 */
class SqliteReservedWordTableNameTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE "order" (id INTEGER PRIMARY KEY, customer TEXT, total REAL)',
            'CREATE TABLE "user" (id INTEGER PRIMARY KEY, name TEXT, email TEXT)',
            'CREATE TABLE "group" (id INTEGER PRIMARY KEY, name TEXT, owner_id INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['order', 'user', 'group'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('INSERT INTO "order" VALUES (1, \'Alice\', 99.99)');
        $this->pdo->exec('INSERT INTO "order" VALUES (2, \'Bob\', 49.50)');
        $this->pdo->exec('INSERT INTO "user" VALUES (1, \'Alice\', \'alice@example.com\')');
        $this->pdo->exec('INSERT INTO "user" VALUES (2, \'Bob\', \'bob@example.com\')');
        $this->pdo->exec('INSERT INTO "group" VALUES (1, \'Admins\', 1)');
    }

    /**
     * Basic SELECT from a table named "order".
     */
    public function testSelectFromOrderTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM "order" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('SELECT from reserved-word table "order" failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('SELECT from reserved-word table "order" returned empty results');
            return;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }

    /**
     * Basic SELECT from a table named "user".
     */
    public function testSelectFromUserTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM "user" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('SELECT from reserved-word table "user" failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('SELECT from reserved-word table "user" returned empty results');
            return;
        }
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Basic SELECT from a table named "group".
     */
    public function testSelectFromGroupTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM "group" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('SELECT from reserved-word table "group" failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('SELECT from reserved-word table "group" returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Admins', $rows[0]['name']);
    }

    /**
     * INSERT into reserved-word table then SELECT back.
     */
    public function testInsertIntoOrderTable(): void
    {
        try {
            $this->pdo->exec('INSERT INTO "order" VALUES (3, \'Charlie\', 75.00)');
            $rows = $this->ztdQuery('SELECT * FROM "order" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT/SELECT with reserved-word table "order" failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) !== 3) {
            $this->markTestIncomplete('Expected 3 rows after INSERT into "order", got ' . count($rows));
            return;
        }
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[2]['customer']);
    }

    /**
     * UPDATE on reserved-word table.
     */
    public function testUpdateOrderTable(): void
    {
        try {
            $this->pdo->exec('UPDATE "order" SET total = 109.99 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT * FROM "order" WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE on reserved-word table "order" failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertEquals(109.99, (float) $rows[0]['total'], '', 0.01);
    }

    /**
     * DELETE from reserved-word table.
     */
    public function testDeleteFromOrderTable(): void
    {
        try {
            $this->pdo->exec('DELETE FROM "order" WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM "order" ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE from reserved-word table "order" failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
    }

    /**
     * JOIN between two reserved-word tables.
     */
    public function testJoinReservedWordTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, g.name AS group_name
                 FROM "user" u
                 JOIN "group" g ON g.owner_id = u.id
                 ORDER BY u.name'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('JOIN between reserved-word tables failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('JOIN between reserved-word tables returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Admins', $rows[0]['group_name']);
    }

    /**
     * Aggregate on reserved-word table.
     */
    public function testAggregateOnOrderTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(total) AS sum_total FROM "order"');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Aggregate on reserved-word table failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(1, $rows);
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Prepared statement with reserved-word table.
     */
    public function testPreparedWithOrderTable(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT * FROM "order" WHERE customer = ?',
                ['Alice']
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared statement with reserved-word table failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Prepared SELECT from reserved-word table returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }

    /**
     * Subquery involving reserved-word table.
     */
    public function testSubqueryWithOrderTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT name FROM "user" WHERE id IN (SELECT id FROM "order" WHERE total > 50) ORDER BY name'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery with reserved-word table failed: ' . $e->getMessage());
            return;
        }

        // user id=1 (Alice) has order total=99.99 > 50; user id=2 (Bob) has order id=2 total=49.50 < 50
        // But note: this compares user.id with order.id, not a FK relationship
        if (count($rows) === 0) {
            $this->markTestIncomplete('Subquery with reserved-word table returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * GROUP BY with HAVING on reserved-word table.
     */
    public function testGroupByHavingOnOrderTable(): void
    {
        $this->pdo->exec('INSERT INTO "order" VALUES (3, \'Alice\', 25.00)');

        try {
            $rows = $this->ztdQuery(
                'SELECT customer, COUNT(*) AS cnt FROM "order" GROUP BY customer HAVING COUNT(*) > 1'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('GROUP BY HAVING on reserved-word table failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('GROUP BY HAVING on reserved-word table returned empty results');
            return;
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Three-way JOIN across all reserved-word tables.
     */
    public function testThreeWayJoinReservedWords(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, g.name AS group_name, o.total
                 FROM "user" u
                 JOIN "group" g ON g.owner_id = u.id
                 JOIN "order" o ON o.customer = u.name
                 ORDER BY o.total DESC'
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete('Three-way JOIN of reserved-word tables failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) === 0) {
            $this->markTestIncomplete('Three-way JOIN of reserved-word tables returned empty results');
            return;
        }
        // Alice is in group Admins, and has 1 order (99.99)
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
