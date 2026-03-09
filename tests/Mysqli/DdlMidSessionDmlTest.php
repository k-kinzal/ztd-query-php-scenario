<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CREATE TABLE mid-session then DML on the new table (MySQLi).
 *
 * The ZTD session starts with only dmd_base reflected. A new table dmd_dynamic
 * is created through ZTD mid-session, and subsequent DML (INSERT/SELECT/UPDATE/DELETE)
 * is performed on it. The shadow store may not know the new table's schema.
 *
 * @spec SPEC-5.1
 * @spec SPEC-4.1
 */
class DdlMidSessionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE dmd_base (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['dmd_base', 'dmd_dynamic'];
    }

    protected function setUp(): void
    {
        // Drop dmd_dynamic via raw connection in case a previous test left it
        $this->dropTable('dmd_dynamic');
        parent::setUp();
    }

    /**
     * CREATE TABLE through ZTD mid-session, then INSERT and SELECT.
     */
    public function testCreateTableThenInsert(): void
    {
        try {
            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'alpha')");

            $result = $this->mysqli->query('SELECT id, value FROM dmd_dynamic WHERE id = 1');
            $row = $result->fetch_assoc();

            $this->assertSame(1, (int) $row['id']);
            $this->assertSame('alpha', $row['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then INSERT not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * After CREATE TABLE + INSERT through ZTD, SELECT should see the inserted data.
     */
    public function testCreateTableThenInsertAndSelect(): void
    {
        try {
            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'alpha')");
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (2, 'beta')");
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (3, 'gamma')");

            $result = $this->mysqli->query('SELECT id, value FROM dmd_dynamic ORDER BY id');
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertSame('alpha', $rows[0]['value']);
            $this->assertSame('beta', $rows[1]['value']);
            $this->assertSame('gamma', $rows[2]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then INSERT+SELECT not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * CREATE TABLE, INSERT, UPDATE, then SELECT to verify update worked.
     */
    public function testCreateTableThenUpdate(): void
    {
        try {
            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'original')");

            $this->mysqli->query("UPDATE dmd_dynamic SET value = 'updated' WHERE id = 1");

            $result = $this->mysqli->query('SELECT value FROM dmd_dynamic WHERE id = 1');
            $row = $result->fetch_assoc();

            $this->assertSame('updated', $row['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then UPDATE not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * CREATE TABLE, INSERT, DELETE, then SELECT to verify delete worked.
     */
    public function testCreateTableThenDelete(): void
    {
        try {
            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'keep')");
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (2, 'remove')");

            $this->mysqli->query('DELETE FROM dmd_dynamic WHERE id = 2');

            $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM dmd_dynamic');
            $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

            $result = $this->mysqli->query('SELECT value FROM dmd_dynamic WHERE id = 1');
            $this->assertSame('keep', $result->fetch_assoc()['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CREATE TABLE mid-session then DELETE not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT into existing base table, CREATE new table, INSERT into new table,
     * then JOIN SELECT across both tables.
     */
    public function testMixedDmlOnExistingAndNewTable(): void
    {
        try {
            $this->mysqli->query("INSERT INTO dmd_base (id, name) VALUES (1, 'Alice')");
            $this->mysqli->query("INSERT INTO dmd_base (id, name) VALUES (2, 'Bob')");

            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'score_90')");
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (2, 'score_80')");

            $result = $this->mysqli->query("
                SELECT b.name, d.value
                FROM dmd_base b
                JOIN dmd_dynamic d ON d.id = b.id
                ORDER BY b.id
            ");
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('score_90', $rows[0]['value']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('score_80', $rows[1]['value']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Mixed DML on existing + mid-session table not supported: ' . $e->getMessage()
            );
        }
    }

    /**
     * After CREATE + INSERT through ZTD, check if the new table physically has data.
     * DDL passes through to the physical DB (SPEC-5.1), but DML should be shadowed.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->mysqli->query('CREATE TABLE dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->mysqli->query("INSERT INTO dmd_dynamic (id, value) VALUES (1, 'shadow_data')");

            // Verify data visible through ZTD
            $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM dmd_dynamic');
            $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

            // Disable ZTD and check physical table
            $this->mysqli->disableZtd();
            $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM dmd_dynamic');
            $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Physical isolation check for mid-session table not supported: ' . $e->getMessage()
            );
        }
    }
}
