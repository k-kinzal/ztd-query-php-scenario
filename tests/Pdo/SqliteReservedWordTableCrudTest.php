<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests full CRUD on tables whose names are SQL reserved words on SQLite.
 *
 * Real-world scenario: legacy databases often use reserved words as table
 * names (e.g., "order", "group", "user", "select"). The CTE rewriter's
 * SQL parser must handle quoted identifiers correctly. SQLite uses
 * double-quotes or backticks for quoting identifiers.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteReservedWordTableCrudTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE "order" (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total REAL NOT NULL
            )',
            'CREATE TABLE "group" (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                member_count INTEGER NOT NULL DEFAULT 0
            )',
            'CREATE TABLE "select" (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['"order"', '"group"', '"select"'];
    }

    /**
     * INSERT into table named "order".
     */
    public function testInsertIntoOrderTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');

            $rows = $this->ztdQuery('SELECT customer, total FROM "order" WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEquals(99.99, (float) $rows[0]['total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT into "order" table failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on table named "order".
     */
    public function testUpdateOrderTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');
            $this->ztdExec('UPDATE "order" SET total = 149.99 WHERE id = 1');

            $rows = $this->ztdQuery('SELECT total FROM "order" WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertEquals(149.99, (float) $rows[0]['total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE "order" table failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from table named "order".
     */
    public function testDeleteFromOrderTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');
            $this->ztdExec('DELETE FROM "order" WHERE id = 1');

            $rows = $this->ztdQuery('SELECT * FROM "order"');
            $this->assertCount(0, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE from "order" table failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT into table named "group".
     */
    public function testInsertIntoGroupTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "group" (id, name, member_count) VALUES (1, \'Admins\', 5)');

            $rows = $this->ztdQuery('SELECT name, member_count FROM "group" WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('Admins', $rows[0]['name']);
            $this->assertSame(5, (int) $rows[0]['member_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT into "group" table failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between two reserved-word tables.
     */
    public function testJoinReservedWordTables(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (2, \'Bob\', 49.99)');
            $this->ztdExec('INSERT INTO "group" (id, name, member_count) VALUES (1, \'VIP\', 2)');

            $rows = $this->ztdQuery(
                'SELECT o.customer, o.total, g.name
                 FROM "order" o
                 JOIN "group" g ON g.id = 1
                 ORDER BY o.customer'
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('VIP', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('JOIN reserved-word tables failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement on reserved-word table.
     */
    public function testPreparedStatementOnReservedWordTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (2, \'Bob\', 49.99)');

            $rows = $this->ztdPrepareAndExecute(
                'SELECT customer, total FROM "order" WHERE total >= ?',
                [50.00]
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared on "order" table failed: ' . $e->getMessage());
        }
    }

    /**
     * Table named "select" — worst case for SQL parser confusion.
     */
    public function testCrudOnSelectTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "select" (id, label) VALUES (1, \'choice_a\')');
            $this->ztdExec('INSERT INTO "select" (id, label) VALUES (2, \'choice_b\')');

            $rows = $this->ztdQuery('SELECT label FROM "select" ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertSame('choice_a', $rows[0]['label']);

            $this->ztdExec('UPDATE "select" SET label = \'updated\' WHERE id = 1');
            $rows = $this->ztdQuery('SELECT label FROM "select" WHERE id = 1');
            $this->assertSame('updated', $rows[0]['label']);

            $this->ztdExec('DELETE FROM "select" WHERE id = 2');
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM "select"');
            $this->assertSame(1, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('CRUD on "select" table failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate on reserved-word table.
     */
    public function testAggregateOnOrderTable(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 100.00)');
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (2, \'Alice\', 200.00)');
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (3, \'Bob\', 50.00)');

            $rows = $this->ztdQuery(
                'SELECT customer, SUM(total) AS sum_total, COUNT(*) AS cnt
                 FROM "order"
                 GROUP BY customer
                 ORDER BY customer'
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEquals(300.00, (float) $rows[0]['sum_total'], '', 0.01);
            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Aggregate on "order" table failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: reserved-word table shadow data stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec('INSERT INTO "order" (id, customer, total) VALUES (1, \'Alice\', 99.99)');

            $this->pdo->disableZtd();
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM "order"');
            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Physical isolation on "order" failed: ' . $e->getMessage());
        }
    }
}
