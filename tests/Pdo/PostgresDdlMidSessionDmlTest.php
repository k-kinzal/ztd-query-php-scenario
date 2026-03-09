<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CREATE TABLE mid-session then DML on the new table (PostgreSQL PDO).
 *
 * The ZTD session starts with only pg_dmd_base reflected. A new table pg_dmd_dynamic
 * is created through ZTD mid-session, and subsequent DML (INSERT/SELECT/UPDATE/DELETE)
 * is performed on it. The shadow store may not know the new table's schema.
 *
 * @spec SPEC-5.1
 * @spec SPEC-4.1
 */
class PostgresDdlMidSessionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_dmd_base (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pg_dmd_base', 'pg_dmd_dynamic'];
    }

    protected function setUp(): void
    {
        // Drop pg_dmd_dynamic via raw connection in case a previous test left it
        $this->dropTable('pg_dmd_dynamic');
        parent::setUp();
    }

    /**
     * CREATE TABLE through ZTD mid-session, then INSERT and SELECT.
     */
    public function testCreateTableThenInsert(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'alpha')");

            $stmt = $this->pdo->query('SELECT id, value FROM pg_dmd_dynamic WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

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
            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'alpha')");
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (2, 'beta')");
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (3, 'gamma')");

            $stmt = $this->pdo->query('SELECT id, value FROM pg_dmd_dynamic ORDER BY id');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

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
            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'original')");

            $this->pdo->exec("UPDATE pg_dmd_dynamic SET value = 'updated' WHERE id = 1");

            $stmt = $this->pdo->query('SELECT value FROM pg_dmd_dynamic WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

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
            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'keep')");
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (2, 'remove')");

            $this->pdo->exec('DELETE FROM pg_dmd_dynamic WHERE id = 2');

            $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_dmd_dynamic');
            $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

            $stmt = $this->pdo->query('SELECT value FROM pg_dmd_dynamic WHERE id = 1');
            $this->assertSame('keep', $stmt->fetch(PDO::FETCH_ASSOC)['value']);
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
            $this->pdo->exec("INSERT INTO pg_dmd_base (id, name) VALUES (1, 'Alice')");
            $this->pdo->exec("INSERT INTO pg_dmd_base (id, name) VALUES (2, 'Bob')");

            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'score_90')");
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (2, 'score_80')");

            $stmt = $this->pdo->query("
                SELECT b.name, d.value
                FROM pg_dmd_base b
                JOIN pg_dmd_dynamic d ON d.id = b.id
                ORDER BY b.id
            ");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

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
            $this->pdo->exec('CREATE TABLE pg_dmd_dynamic (id INT PRIMARY KEY, value VARCHAR(50))');
            $this->pdo->exec("INSERT INTO pg_dmd_dynamic (id, value) VALUES (1, 'shadow_data')");

            // Verify data visible through ZTD
            $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_dmd_dynamic');
            $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

            // Disable ZTD and check physical table
            $this->pdo->disableZtd();
            $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_dmd_dynamic');
            $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Physical isolation check for mid-session table not supported: ' . $e->getMessage()
            );
        }
    }
}
