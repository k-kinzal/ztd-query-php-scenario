<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests ZTD behavior with quoted identifiers and SQL reserved words as
 * column/table names. Real-world schemas often use reserved words like
 * "order", "group", "select", "key", "value" as identifiers.
 * @spec SPEC-4.9
 */
class SqliteQuotedIdentifierTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE qi_items (
            "id" INTEGER PRIMARY KEY,
            "order" INTEGER,
            "group" TEXT,
            "key" TEXT,
            "value" TEXT,
            "select" TEXT
        )',
            'CREATE TABLE "order" ("id" INTEGER PRIMARY KEY, "status" TEXT, "total" REAL)',
            'CREATE TABLE qi_parent ("id" INTEGER PRIMARY KEY, "key" TEXT)',
            'CREATE TABLE qi_child ("id" INTEGER PRIMARY KEY, "key" TEXT, "value" TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['qi_items', 'order', 'qi_parent', 'qi_child'];
    }


    /**
     * INSERT and SELECT with quoted reserved-word columns.
     */
    public function testInsertAndSelectWithReservedWordColumns(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 10, \'A\', \'k1\', \'v1\', \'s1\')');

        $stmt = $this->pdo->query('SELECT "order", "group", "key", "value", "select" FROM qi_items WHERE "id" = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(10, (int) $row['order']);
        $this->assertSame('A', $row['group']);
        $this->assertSame('k1', $row['key']);
        $this->assertSame('v1', $row['value']);
        $this->assertSame('s1', $row['select']);
    }

    /**
     * UPDATE with reserved-word columns.
     */
    public function testUpdateReservedWordColumns(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 10, \'A\', \'k1\', \'v1\', \'s1\')');

        $this->pdo->exec('UPDATE qi_items SET "value" = \'updated\', "order" = 20 WHERE "id" = 1');

        $stmt = $this->pdo->query('SELECT "value", "order" FROM qi_items WHERE "id" = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['value']);
        $this->assertSame(20, (int) $row['order']);
    }

    /**
     * DELETE with WHERE on reserved-word column.
     */
    public function testDeleteWithReservedWordWhere(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 10, \'A\', \'k1\', \'v1\', \'s1\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (2, 20, \'B\', \'k2\', \'v2\', \'s2\')');

        $affected = $this->pdo->exec('DELETE FROM qi_items WHERE "group" = \'A\'');
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM qi_items');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * GROUP BY on a column named "group".
     */
    public function testGroupByOnColumnNamedGroup(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 10, \'A\', \'k1\', \'v1\', \'s1\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (2, 20, \'A\', \'k2\', \'v2\', \'s2\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (3, 30, \'B\', \'k3\', \'v3\', \'s3\')');

        $stmt = $this->pdo->query('SELECT "group", COUNT(*) AS cnt FROM qi_items GROUP BY "group" ORDER BY "group"');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['group']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame('B', $rows[1]['group']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    /**
     * ORDER BY on a column named "order".
     */
    public function testOrderByOnColumnNamedOrder(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 30, \'A\', \'k1\', \'v1\', \'s1\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (2, 10, \'B\', \'k2\', \'v2\', \'s2\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (3, 20, \'C\', \'k3\', \'v3\', \'s3\')');

        $stmt = $this->pdo->query('SELECT "id", "order" FROM qi_items ORDER BY "order" ASC');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
        $this->assertSame(1, (int) $rows[2]['id']);
    }

    /**
     * Prepared statement with reserved-word column as parameter.
     */
    public function testPreparedStatementWithReservedWordColumn(): void
    {
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (1, 10, \'A\', \'k1\', \'v1\', \'s1\')');
        $this->pdo->exec('INSERT INTO qi_items ("id", "order", "group", "key", "value", "select") VALUES (2, 20, \'B\', \'k2\', \'v2\', \'s2\')');

        $stmt = $this->pdo->prepare('SELECT * FROM qi_items WHERE "group" = ?');
        $stmt->execute(['A']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('k1', $rows[0]['key']);
    }

    /**
     * Table name that is a reserved word.
     */
    public function testTableNameIsReservedWord(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE "order" ("id" INTEGER PRIMARY KEY, "status" TEXT, "total" REAL)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec('INSERT INTO "order" ("id", "status", "total") VALUES (1, \'pending\', 99.99)');

        $stmt = $pdo->query('SELECT * FROM "order" WHERE "id" = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('pending', $row['status']);
        $this->assertEqualsWithDelta(99.99, (float) $row['total'], 0.01);
    }

    /**
     * JOIN between tables using reserved-word columns.
     */
    public function testJoinOnReservedWordColumns(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE qi_parent ("id" INTEGER PRIMARY KEY, "key" TEXT)');
        $raw->exec('CREATE TABLE qi_child ("id" INTEGER PRIMARY KEY, "key" TEXT, "value" TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec('INSERT INTO qi_parent VALUES (1, \'k1\')');
        $pdo->exec('INSERT INTO qi_child VALUES (1, \'k1\', \'child_val\')');

        $stmt = $pdo->query('SELECT p."key", c."value" FROM qi_parent p JOIN qi_child c ON c."key" = p."key"');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('k1', $row['key']);
        $this->assertSame('child_val', $row['value']);
    }
}
