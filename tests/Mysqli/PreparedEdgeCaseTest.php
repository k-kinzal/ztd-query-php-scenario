<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared statement edge cases in ZTD mode on MySQL via MySQLi:
 * NULL binding, re-execution, multiple conditions, prepared UPDATE/DELETE,
 * and statement reuse patterns.
 * @spec SPEC-3.2
 */
class PreparedEdgeCaseTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_prep_edge (id INT PRIMARY KEY, name VARCHAR(255), score INT, active TINYINT(1))';
    }

    protected function getTableNames(): array
    {
        return ['mi_prep_edge'];
    }


    public function testBindParamNullType(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = null;
        $stmt->bind_param('iss', $id, $name, $score);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT * FROM mi_prep_edge WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertNull($row['score']);
    }

    public function testReExecuteWithDifferentParams(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $id = 0;
        $name = '';
        $score = 0;
        $stmt->bind_param('isi', $id, $name, $score);

        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->execute();

        $id = 2;
        $name = 'Bob';
        $score = 85;
        $stmt->execute();

        $id = 3;
        $name = 'Charlie';
        $score = 70;
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_prep_edge');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    public function testPreparedSelectWithMultipleConditions(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score, active) VALUES (1, 'Alice', 100, 1)");
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score, active) VALUES (2, 'Bob', 85, 1)");
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score, active) VALUES (3, 'Charlie', 70, 0)");

        $stmt = $this->mysqli->prepare('SELECT name FROM mi_prep_edge WHERE score >= ? AND active = ? ORDER BY name');
        $minScore = 80;
        $active = 1;
        $stmt->bind_param('ii', $minScore, $active);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testPreparedUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->mysqli->prepare('UPDATE mi_prep_edge SET score = ? WHERE id = ?');
        $newScore = 95;
        $id = 1;
        $stmt->bind_param('ii', $newScore, $id);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $result = $this->mysqli->query('SELECT score FROM mi_prep_edge WHERE id = 1');
        $this->assertSame(95, (int) $result->fetch_assoc()['score']);
    }

    public function testPreparedDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_prep_edge (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->mysqli->prepare('DELETE FROM mi_prep_edge WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_prep_edge');
        $this->assertSame(1, (int) $result->fetch_assoc()['c']);
    }

    public function testPreparedInsertThenSelectReusePrepared(): void
    {
        $insertStmt = $this->mysqli->prepare('INSERT INTO mi_prep_edge (id, name, score) VALUES (?, ?, ?)');
        $id = 0;
        $name = '';
        $score = 0;
        $insertStmt->bind_param('isi', $id, $name, $score);

        $id = 1;
        $name = 'Alice';
        $score = 100;
        $insertStmt->execute();

        $id = 2;
        $name = 'Bob';
        $score = 85;
        $insertStmt->execute();

        $selectStmt = $this->mysqli->prepare('SELECT name FROM mi_prep_edge WHERE id = ?');
        $selectId = 0;
        $selectStmt->bind_param('i', $selectId);

        $selectId = 1;
        $selectStmt->execute();
        $result = $selectStmt->get_result();
        $this->assertSame('Alice', $result->fetch_assoc()['name']);

        $selectId = 2;
        $selectStmt->execute();
        $result = $selectStmt->get_result();
        $this->assertSame('Bob', $result->fetch_assoc()['name']);
    }

    public function testExecuteQueryWithNull(): void
    {
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $this->mysqli->execute_query(
            'INSERT INTO mi_prep_edge (id, name, score) VALUES (?, ?, ?)',
            [1, 'Alice', null]
        );

        $result = $this->mysqli->query('SELECT * FROM mi_prep_edge WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertNull($row['score']);
    }
}
