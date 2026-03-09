<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests closeCursor() followed by re-execution with shadow mutations between calls.
 *
 * The ZtdPdoStatement stores a $result from execution. closeCursor() delegates
 * to the inner statement but may not reset internal state. After shadow mutations,
 * re-execution should return fresh results.
 *
 * Note: Failures here (testCloseCursorThenMutateThenReexecute,
 * testCloseCursorThenInsertThenReexecute, testMultipleCloseCursorCycles) are
 * caused by the broader "prepared SELECT re-execute stale shadow data" issue —
 * see SqlitePreparedSelectReexecuteStaleTest for focused coverage.
 *
 * @spec SPEC-3.2
 */
class SqliteCloseCursorReexecuteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE cc_data (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['cc_data'];
    }

    /**
     * closeCursor on SELECT, mutate, re-execute same prepared statement.
     */
    public function testCloseCursorThenMutateThenReexecute(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'original', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM cc_data WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('original', $row['val']);
        $stmt->closeCursor();

        // Mutate via another statement
        $this->pdo->exec("UPDATE cc_data SET val = 'updated' WHERE id = 1");

        // Re-execute the same prepared statement
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['val']);
    }

    /**
     * closeCursor on SELECT, insert new row, re-execute with different param.
     */
    public function testCloseCursorThenInsertThenReexecute(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'first', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM cc_data WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('first', $row['val']);
        $stmt->closeCursor();

        // Insert a new row
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (2, 'second', 20)");

        // Re-execute looking for the new row
        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
        $this->assertSame('second', $row['val']);
    }

    /**
     * closeCursor on INSERT prepared statement, then re-execute.
     */
    public function testCloseCursorOnInsertThenReexecute(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO cc_data (id, val, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'first', 10]);
        $stmt->closeCursor();

        $stmt->execute([2, 'second', 20]);
        $stmt->closeCursor();

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM cc_data');
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * closeCursor on UPDATE, then re-execute with different params.
     */
    public function testCloseCursorOnUpdateThenReexecute(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('UPDATE cc_data SET score = ? WHERE id = ?');
        $stmt->execute([100, 1]);
        $stmt->closeCursor();

        $stmt->execute([200, 2]);
        $stmt->closeCursor();

        $rows = $this->ztdQuery('SELECT id, score FROM cc_data ORDER BY id');
        $this->assertEquals(100, (int) $rows[0]['score']);
        $this->assertEquals(200, (int) $rows[1]['score']);
    }

    /**
     * Multiple closeCursor/re-execute cycles on same SELECT.
     */
    public function testMultipleCloseCursorCycles(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'a', 10)");

        $stmt = $this->pdo->prepare('SELECT score FROM cc_data WHERE id = 1');

        for ($i = 0; $i < 3; $i++) {
            // Update score
            $this->pdo->exec("UPDATE cc_data SET score = " . ($i * 100) . " WHERE id = 1");

            $stmt->execute();
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertEquals($i * 100, (int) $row['score'], "Cycle $i");
            $stmt->closeCursor();
        }
    }

    /**
     * closeCursor on DELETE, then re-execute.
     */
    public function testCloseCursorOnDeleteThenReexecute(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $stmt = $this->pdo->prepare('DELETE FROM cc_data WHERE id = ?');
        $stmt->execute([1]);
        $stmt->closeCursor();

        $stmt->execute([2]);
        $stmt->closeCursor();

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM cc_data');
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * rowCount() after closeCursor and re-execute.
     */
    public function testRowCountAfterCloseCursorReexecute(): void
    {
        $this->pdo->exec("INSERT INTO cc_data (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('UPDATE cc_data SET score = ? WHERE score < ?');
        $stmt->execute([100, 15]);
        $count1 = $stmt->rowCount();
        $stmt->closeCursor();

        $stmt->execute([200, 150]);
        $count2 = $stmt->rowCount();

        $this->assertEquals(1, $count1);
        $this->assertEquals(2, $count2);
    }
}
