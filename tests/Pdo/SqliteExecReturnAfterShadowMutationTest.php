<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests exec() return value and rowCount() accuracy after shadow mutations.
 *
 * exec() should return the number of affected rows for DML statements.
 * This tests edge cases where shadow store mutations affect the row count
 * in unexpected ways.
 *
 * @spec SPEC-4.10
 */
class SqliteExecReturnAfterShadowMutationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE erc (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['erc'];
    }

    /**
     * exec() returns 0 for UPDATE that matches no rows.
     */
    public function testExecReturnsZeroForNoMatchUpdate(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES (1, 'a', 10)");

        $affected = $this->pdo->exec("UPDATE erc SET score = 99 WHERE id = 999");
        $this->assertEquals(0, $affected);
    }

    /**
     * exec() returns 0 for DELETE that matches no rows.
     */
    public function testExecReturnsZeroForNoMatchDelete(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES (1, 'a', 10)");

        $affected = $this->pdo->exec("DELETE FROM erc WHERE id = 999");
        $this->assertEquals(0, $affected);
    }

    /**
     * exec() returns correct count for multi-row UPDATE.
     */
    public function testExecReturnsCorrectCountMultiRowUpdate(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $affected = $this->pdo->exec("UPDATE erc SET score = 99 WHERE score > 10");
        $this->assertEquals(2, $affected);
    }

    /**
     * exec() after DELETE then re-INSERT same ID.
     */
    public function testExecAfterDeleteReinsert(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES (1, 'original', 10)");
        $this->pdo->exec("DELETE FROM erc WHERE id = 1");

        $affected = $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES (1, 'reinserted', 20)");
        // INSERT should return row count (may be 0 or 1 depending on whether exec returns for INSERT)
        $this->assertIsInt($affected);

        $rows = $this->ztdQuery('SELECT val FROM erc WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('reinserted', $rows[0]['val']);
    }

    /**
     * UPDATE WHERE with complex condition on shadow data.
     */
    public function testUpdateWhereComplexCondition(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES
            (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)");

        $affected = $this->pdo->exec("UPDATE erc SET val = 'updated' WHERE score BETWEEN 20 AND 30");
        $this->assertEquals(2, $affected);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM erc WHERE val = 'updated'");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * DELETE with IN clause.
     */
    public function testDeleteWithInClause(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES
            (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)");

        $affected = $this->pdo->exec("DELETE FROM erc WHERE id IN (2, 4)");
        $this->assertEquals(2, $affected);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM erc');
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * rowCount() on prepared UPDATE with complex WHERE.
     */
    public function testRowCountPreparedUpdateComplexWhere(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES
            (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $stmt = $this->pdo->prepare('UPDATE erc SET val = ? WHERE score >= ?');
        $stmt->execute(['high', 20]);
        $this->assertEquals(2, $stmt->rowCount());
    }

    /**
     * Sequential UPDATE then DELETE — rowCount reflects each operation.
     */
    public function testSequentialRowCounts(): void
    {
        $this->pdo->exec("INSERT INTO erc (id, val, score) VALUES
            (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");

        $updateAffected = $this->pdo->exec("UPDATE erc SET score = 99 WHERE id = 1");
        $this->assertEquals(1, $updateAffected);

        $deleteAffected = $this->pdo->exec("DELETE FROM erc WHERE score < 30");
        $this->assertEquals(1, $deleteAffected); // Only id=2 has score<30 now (id=1 was updated to 99)
    }
}
