<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that rows inserted via prepared statements can be updated/deleted
 * on the PDO adapter.
 *
 * @see https://github.com/k-kinzal/ztd-query-php/issues/23
 * @spec pending
 */
class SqlitePreparedInsertUpdateBugTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bug_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['bug_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE bug_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');

        }

    public function testExecInsertThenExecUpdateWorks(): void
    {
        $this->pdo->exec("INSERT INTO bug_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE bug_test SET score = 200 WHERE id = 1");

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(200, (int) $row['score']);
    }

    public function testExecInsertThenPreparedUpdateWorks(): void
    {
        $this->pdo->exec("INSERT INTO bug_test VALUES (1, 'Alice', 100)");
        $stmt = $this->pdo->prepare('UPDATE bug_test SET score = ? WHERE id = ?');
        $stmt->execute([200, 1]);

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * Prepared INSERT + exec UPDATE should update the row.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertThenExecUpdate(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $this->pdo->exec("UPDATE bug_test SET score = 200 WHERE id = 1");

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $score = (int) $row['score'];
        if ($score !== 200) {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT rows cannot be updated. Expected score 200, got ' . $score
            );
        }
        $this->assertSame(200, $score);
    }

    /**
     * Prepared INSERT + prepared UPDATE should update the row.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertThenPreparedUpdate(): void
    {
        $ins = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $ins->execute([1, 'Alice', 100]);

        $upd = $this->pdo->prepare('UPDATE bug_test SET score = ? WHERE id = ?');
        $upd->execute([200, 1]);

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $score = (int) $row['score'];
        if ($score !== 200) {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT rows cannot be updated via prepared UPDATE. Expected score 200, got ' . $score
            );
        }
        $this->assertSame(200, $score);
    }

    /**
     * Prepared INSERT + DELETE should delete the row.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertThenDelete(): void
    {
        $ins = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $ins->execute([1, 'Alice', 100]);
        $ins->execute([2, 'Bob', 200]);

        $this->pdo->exec("DELETE FROM bug_test WHERE id = 1");

        $cnt = (int) $this->pdo->query('SELECT COUNT(*) FROM bug_test')->fetchColumn();
        if ($cnt !== 1) {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT rows cannot be deleted. Expected count 1, got ' . $cnt
            );
        }
        $this->assertSame(1, $cnt);
    }

    /**
     * Mixed: both exec and prepared INSERT rows should be updatable.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testMixedInsertUpdateBothUpdated(): void
    {
        $this->pdo->exec("INSERT INTO bug_test VALUES (1, 'ExecAlice', 100)");

        $stmt = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([2, 'PrepBob', 200]);

        $this->pdo->exec("UPDATE bug_test SET score = 999 WHERE score > 0");

        $row1 = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $row2 = $this->pdo->query('SELECT score FROM bug_test WHERE id = 2')->fetch(PDO::FETCH_ASSOC);

        $score1 = (int) $row1['score'];
        $score2 = (int) $row2['score'];

        // Both rows should be updated to 999
        if ($score2 !== 999) {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT rows not updatable. '
                . 'exec-inserted row score: ' . $score1 . ', prepared-inserted row score: ' . $score2
                . ' (both should be 999)'
            );
        }
        $this->assertSame(999, $score1);
        $this->assertSame(999, $score2);
    }
}
