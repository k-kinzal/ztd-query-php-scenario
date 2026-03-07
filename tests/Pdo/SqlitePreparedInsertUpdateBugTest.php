<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Documents bug: rows inserted via prepared statements cannot be updated/deleted
 * on the PDO adapter (issue #23).
 *
 * UPDATE/DELETE operations report affected rows, but data doesn't change.
 * Rows inserted via exec() work correctly.
 * MySQLi adapter is NOT affected.
 */
class SqlitePreparedInsertUpdateBugTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

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
     * Bug: prepared INSERT + exec UPDATE — update does not take effect.
     */
    public function testPreparedInsertThenExecUpdateFails(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $this->pdo->exec("UPDATE bug_test SET score = 200 WHERE id = 1");

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        // Bug: should be 200, but old value is retained
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * Bug: prepared INSERT + prepared UPDATE — update does not take effect.
     */
    public function testPreparedInsertThenPreparedUpdateFails(): void
    {
        $ins = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $ins->execute([1, 'Alice', 100]);

        $upd = $this->pdo->prepare('UPDATE bug_test SET score = ? WHERE id = ?');
        $upd->execute([200, 1]);

        $row = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        // Bug: should be 200, but old value is retained
        $this->assertSame(100, (int) $row['score']);
    }

    /**
     * Bug: prepared INSERT + DELETE — delete does not take effect.
     */
    public function testPreparedInsertThenDeleteFails(): void
    {
        $ins = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $ins->execute([1, 'Alice', 100]);
        $ins->execute([2, 'Bob', 200]);

        $this->pdo->exec("DELETE FROM bug_test WHERE id = 1");

        $cnt = (int) $this->pdo->query('SELECT COUNT(*) FROM bug_test')->fetchColumn();
        // Bug: should be 1 (Alice deleted), but both rows persist
        $this->assertSame(2, $cnt);
    }

    /**
     * Mixed: exec INSERT works, prepared INSERT cannot be updated.
     */
    public function testMixedInsertUpdateSelectivelyFails(): void
    {
        $this->pdo->exec("INSERT INTO bug_test VALUES (1, 'ExecAlice', 100)");

        $stmt = $this->pdo->prepare('INSERT INTO bug_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([2, 'PrepBob', 200]);

        $this->pdo->exec("UPDATE bug_test SET score = 999 WHERE score > 0");

        $row1 = $this->pdo->query('SELECT score FROM bug_test WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $row2 = $this->pdo->query('SELECT score FROM bug_test WHERE id = 2')->fetch(PDO::FETCH_ASSOC);

        // exec-inserted row gets updated
        $this->assertSame(999, (int) $row1['score']);
        // prepared-inserted row does NOT get updated
        $this->assertSame(200, (int) $row2['score']);
    }
}
