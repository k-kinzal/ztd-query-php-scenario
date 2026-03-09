<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests bindParam (pass-by-reference) with DML mutations between re-executions.
 *
 * bindParam binds a variable by reference. When the reference changes between
 * calls to execute(), the shadow store must pick up the updated value. This
 * tests the interaction between reference binding and CTE rewriting.
 *
 * @spec SPEC-4.2
 */
class SqliteBindParamDmlReexecuteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bp_ref (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['bp_ref'];
    }

    /**
     * bindParam with UPDATE — reference changes between executions.
     */
    public function testBindParamUpdateWithReferenceChange(): void
    {
        $this->pdo->exec("INSERT INTO bp_ref (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $stmt = $this->pdo->prepare('UPDATE bp_ref SET score = ? WHERE id = ?');
        $score = 100;
        $id = 1;
        $stmt->bindParam(1, $score, PDO::PARAM_INT);
        $stmt->bindParam(2, $id, PDO::PARAM_INT);
        $stmt->execute();

        // Change references and re-execute
        $score = 200;
        $id = 2;
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT id, score FROM bp_ref ORDER BY id');
        $this->assertEquals(100, (int) $rows[0]['score']);
        $this->assertEquals(200, (int) $rows[1]['score']);
        $this->assertEquals(30, (int) $rows[2]['score']);
    }

    /**
     * bindParam with INSERT — reference changes between executions.
     */
    public function testBindParamInsertWithReferenceChange(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bp_ref (id, val, score) VALUES (?, ?, ?)');
        $id = 1;
        $val = 'first';
        $score = 10;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->bindParam(2, $val, PDO::PARAM_STR);
        $stmt->bindParam(3, $score, PDO::PARAM_INT);
        $stmt->execute();

        $id = 2;
        $val = 'second';
        $score = 20;
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT id, val, score FROM bp_ref ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('first', $rows[0]['val']);
        $this->assertSame('second', $rows[1]['val']);
        $this->assertEquals(10, (int) $rows[0]['score']);
        $this->assertEquals(20, (int) $rows[1]['score']);
    }

    /**
     * bindParam with DELETE — reference changes between executions.
     */
    public function testBindParamDeleteWithReferenceChange(): void
    {
        $this->pdo->exec("INSERT INTO bp_ref (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $stmt = $this->pdo->prepare('DELETE FROM bp_ref WHERE id = ?');
        $id = 1;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->execute();

        $id = 3;
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT id FROM bp_ref');
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['id']);
    }

    /**
     * bindParam with SELECT — reference changes between executions.
     */
    public function testBindParamSelectWithReferenceChange(): void
    {
        $this->pdo->exec("INSERT INTO bp_ref (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('SELECT val FROM bp_ref WHERE id = ?');
        $id = 1;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('a', $row['val']);
        $stmt->closeCursor();

        // Change reference
        $id = 2;
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('b', $row['val']);
    }

    /**
     * bindParam INSERT then SELECT the inserted data via different statement.
     */
    public function testBindParamInsertThenSelectVerify(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bp_ref (id, val, score) VALUES (?, ?, ?)');
        $id = 0;
        $val = '';
        $score = 0;
        $stmt->bindParam(1, $id, PDO::PARAM_INT);
        $stmt->bindParam(2, $val, PDO::PARAM_STR);
        $stmt->bindParam(3, $score, PDO::PARAM_INT);

        for ($i = 1; $i <= 5; $i++) {
            $id = $i;
            $val = "item_$i";
            $score = $i * 10;
            $stmt->execute();
        }

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM bp_ref');
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery('SELECT val, score FROM bp_ref WHERE id = 3');
        $this->assertSame('item_3', $rows[0]['val']);
        $this->assertEquals(30, (int) $rows[0]['score']);
    }

    /**
     * Named bindParam with DML re-execution.
     */
    public function testNamedBindParamWithReexecution(): void
    {
        $this->pdo->exec("INSERT INTO bp_ref (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('UPDATE bp_ref SET score = :score WHERE id = :id');
        $score = 100;
        $id = 1;
        $stmt->bindParam(':score', $score, PDO::PARAM_INT);
        $stmt->bindParam(':id', $id, PDO::PARAM_INT);
        $stmt->execute();

        $score = 200;
        $id = 2;
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT id, score FROM bp_ref ORDER BY id');
        $this->assertEquals(100, (int) $rows[0]['score']);
        $this->assertEquals(200, (int) $rows[1]['score']);
    }
}
