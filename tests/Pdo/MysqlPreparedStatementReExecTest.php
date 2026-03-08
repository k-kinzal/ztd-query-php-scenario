<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared statement re-execution patterns on MySQL ZTD PDO.
 * @spec pending
 */
class MysqlPreparedStatementReExecTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE re_exec_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['re_exec_m'];
    }


    public function testReExecuteSelectWithDifferentParams(): void
    {
        $this->pdo->exec("INSERT INTO re_exec_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO re_exec_m VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name FROM re_exec_m WHERE id = ?');

        $stmt->execute([1]);
        $this->assertSame('Alice', $stmt->fetchColumn());

        $stmt->execute([2]);
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    public function testReExecuteInsertMultipleTimes(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO re_exec_m (id, name, score) VALUES (?, ?, ?)');

        $stmt->execute([1, 'Alice', 100]);
        $stmt->execute([2, 'Bob', 85]);
        $stmt->execute([3, 'Charlie', 70]);

        $count = $this->pdo->query('SELECT COUNT(*) FROM re_exec_m')->fetchColumn();
        $this->assertSame(3, (int) $count);
    }

    public function testReExecuteUpdateWithDifferentValues(): void
    {
        $this->pdo->exec("INSERT INTO re_exec_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO re_exec_m VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('UPDATE re_exec_m SET score = ? WHERE id = ?');

        $stmt->execute([200, 1]);
        $stmt->execute([300, 2]);

        $row1 = $this->pdo->query('SELECT score FROM re_exec_m WHERE id = 1')->fetch(PDO::FETCH_ASSOC);
        $row2 = $this->pdo->query('SELECT score FROM re_exec_m WHERE id = 2')->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(200, (int) $row1['score']);
        $this->assertSame(300, (int) $row2['score']);
    }

    public function testPreparedSelectSeesEarlierPreparedInsert(): void
    {
        // Prepared INSERT + then SELECT prepared AFTER
        $ins = $this->pdo->prepare('INSERT INTO re_exec_m (id, name, score) VALUES (?, ?, ?)');
        $ins->execute([1, 'Alice', 100]);

        $sel = $this->pdo->prepare('SELECT name FROM re_exec_m WHERE id = ?');
        $sel->execute([1]);
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }
}
