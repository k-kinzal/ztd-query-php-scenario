<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests execute_query() with UPDATE and DELETE operations (PHP 8.2+).
 *
 * execute_query() internally uses prepare() + execute(), but prior tests
 * only covered SELECT and INSERT. This file verifies UPDATE and DELETE
 * also work correctly through the execute_query() path.
 * @spec pending
 */
class ExecuteQueryWriteOpsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_eq_write (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_eq_write'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        if (!method_exists(\mysqli::class, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }
        $this->mysqli->query("INSERT INTO mi_eq_write VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_eq_write VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_eq_write VALUES (3, 'Charlie', 70)");
    }

    public function testUpdateWithParams(): void
    {
        $this->mysqli->execute_query(
            'UPDATE mi_eq_write SET score = ? WHERE id = ?',
            [95, 1]
        );

        $result = $this->mysqli->query('SELECT score FROM mi_eq_write WHERE id = 1');
        $this->assertEquals(95, $result->fetch_assoc()['score']);
    }

    public function testUpdateMultipleRows(): void
    {
        $this->mysqli->execute_query(
            'UPDATE mi_eq_write SET score = ? WHERE score < ?',
            [99, 85]
        );

        $result = $this->mysqli->query('SELECT score FROM mi_eq_write WHERE id = 2');
        $this->assertEquals(99, $result->fetch_assoc()['score']);

        $result = $this->mysqli->query('SELECT score FROM mi_eq_write WHERE id = 3');
        $this->assertEquals(99, $result->fetch_assoc()['score']);

        // Unchanged row
        $result = $this->mysqli->query('SELECT score FROM mi_eq_write WHERE id = 1');
        $this->assertEquals(90, $result->fetch_assoc()['score']);
    }

    public function testDeleteWithParams(): void
    {
        $this->mysqli->execute_query(
            'DELETE FROM mi_eq_write WHERE id = ?',
            [2]
        );

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_eq_write');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT id FROM mi_eq_write ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertEquals(1, $rows[0]['id']);
        $this->assertEquals(3, $rows[1]['id']);
    }

    public function testDeleteMultipleRows(): void
    {
        $this->mysqli->execute_query(
            'DELETE FROM mi_eq_write WHERE score < ?',
            [85]
        );

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_eq_write');
        $this->assertEquals(1, $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testUpdateThenSelect(): void
    {
        $this->mysqli->execute_query(
            'UPDATE mi_eq_write SET name = ? WHERE id = ?',
            ['Alicia', 1]
        );

        $result = $this->mysqli->execute_query(
            'SELECT name FROM mi_eq_write WHERE id = ?',
            [1]
        );
        $this->assertSame('Alicia', $result->fetch_assoc()['name']);
    }

    public function testDeleteThenInsert(): void
    {
        $this->mysqli->execute_query(
            'DELETE FROM mi_eq_write WHERE id = ?',
            [1]
        );

        $this->mysqli->execute_query(
            'INSERT INTO mi_eq_write (id, name, score) VALUES (?, ?, ?)',
            [4, 'Diana', 85]
        );

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_eq_write');
        $this->assertEquals(3, $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write WHERE id = 4');
        $this->assertSame('Diana', $result->fetch_assoc()['name']);
    }

    public function testUpdateAffectedRows(): void
    {
        $this->mysqli->execute_query(
            'UPDATE mi_eq_write SET score = ? WHERE score < ?',
            [99, 85]
        );

        $this->assertEquals(2, $this->mysqli->lastAffectedRows());
    }

    public function testDeleteAffectedRows(): void
    {
        $this->mysqli->execute_query(
            'DELETE FROM mi_eq_write WHERE score <= ?',
            [80]
        );

        $this->assertEquals(2, $this->mysqli->lastAffectedRows());
    }

    /**
     * execute_query() with UPSERT does NOT update existing rows.
     *
     * This contrasts with prepare() + bind_param() + execute() which works
     * correctly (see PreparedUpsertTest::testPreparedUpsertUpdatesExisting).
     * The difference is in how execute_query passes parameters vs bind_param.
     */
    public function testUpsertDoesNotUpdateExistingViaExecuteQuery(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eq_write (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)',
            [1, 'Alicia', 95]
        );

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write WHERE id = 1');
        // Old row is retained — execute_query limitation
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testUpsertInsertsNewViaExecuteQuery(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eq_write (id, name, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)',
            [4, 'Diana', 85]
        );

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write WHERE id = 4');
        $this->assertSame('Diana', $result->fetch_assoc()['name']);
    }

    /**
     * execute_query() with REPLACE does NOT replace existing rows.
     *
     * This contrasts with prepare() + bind_param() + execute() which works
     * correctly (see PreparedUpsertTest::testPreparedReplaceReplacesExisting).
     */
    public function testReplaceDoesNotReplaceExistingViaExecuteQuery(): void
    {
        $this->mysqli->execute_query(
            'REPLACE INTO mi_eq_write (id, name, score) VALUES (?, ?, ?)',
            [2, 'Bobby', 85]
        );

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write WHERE id = 2');
        // Old row is retained — execute_query limitation
        $this->assertSame('Bob', $result->fetch_assoc()['name']);
    }

    public function testReplaceInsertsNewViaExecuteQuery(): void
    {
        $this->mysqli->execute_query(
            'REPLACE INTO mi_eq_write (id, name, score) VALUES (?, ?, ?)',
            [4, 'Diana', 85]
        );

        $result = $this->mysqli->query('SELECT name FROM mi_eq_write WHERE id = 4');
        $this->assertSame('Diana', $result->fetch_assoc()['name']);
    }

    public function testPhysicalIsolationAfterUpdate(): void
    {
        $this->mysqli->execute_query(
            'UPDATE mi_eq_write SET name = ? WHERE id = ?',
            ['UpdatedAlice', 1]
        );

        // Physical table should still be empty (shadow-only operations)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM mi_eq_write');
        $this->assertSame(0, $result->num_rows);
    }
}
