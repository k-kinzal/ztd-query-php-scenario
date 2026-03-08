<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Confirms MySQLi adapter correctly handles prepared INSERT + subsequent UPDATE.
 * (PDO adapter has issue #23 — MySQLi is NOT affected.)
 * @spec pending
 */
class PreparedInsertUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_prep_ins (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_prep_ins'];
    }


    public function testPreparedInsertThenQueryUpdateWorks(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_ins (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $this->mysqli->query("UPDATE mi_prep_ins SET score = 200 WHERE id = 1");

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins WHERE id = 1');
        $row = $result->fetch_assoc();
        // MySQLi works correctly — no issue #23
        $this->assertSame(200, (int) $row['score']);
    }

    public function testPreparedInsertThenPreparedUpdateWorks(): void
    {
        $ins = $this->mysqli->prepare('INSERT INTO mi_prep_ins (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $ins->bind_param('isi', $id, $name, $score);
        $ins->execute();

        $upd = $this->mysqli->prepare('UPDATE mi_prep_ins SET score = ? WHERE id = ?');
        $newScore = 200;
        $updId = 1;
        $upd->bind_param('ii', $newScore, $updId);
        $upd->execute();

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(200, (int) $row['score']);
    }

    public function testPreparedInsertThenDeleteWorks(): void
    {
        $ins = $this->mysqli->prepare('INSERT INTO mi_prep_ins (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $ins->bind_param('isi', $id, $name, $score);
        $ins->execute();

        $id = 2;
        $name = 'Bob';
        $score = 200;
        $ins->execute();

        $this->mysqli->query("DELETE FROM mi_prep_ins WHERE id = 1");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_prep_ins');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }
}
