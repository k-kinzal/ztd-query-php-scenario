<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PARAM_BOOL type coercion through prepared statements with ZTD.
 *
 * Boolean parameter handling can differ between native PDO and the ZTD
 * CTE rewriter. The rewriter must correctly handle boolean values in
 * WHERE clauses, INSERT VALUES, and UPDATE SET expressions.
 *
 * @spec SPEC-3.2
 */
class SqliteParamBoolCoercionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bool_t (id INTEGER PRIMARY KEY, name TEXT, active INTEGER, verified INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['bool_t'];
    }

    /**
     * INSERT with PARAM_BOOL true.
     */
    public function testInsertWithParamBoolTrue(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bool_t (id, name, active, verified) VALUES (?, ?, ?, ?)');
        $stmt->bindValue(1, 1, PDO::PARAM_INT);
        $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
        $stmt->bindValue(3, true, PDO::PARAM_BOOL);
        $stmt->bindValue(4, false, PDO::PARAM_BOOL);
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT active, verified FROM bool_t WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['active']);
        $this->assertEquals(0, (int) $rows[0]['verified']);
    }

    /**
     * SELECT with PARAM_BOOL in WHERE.
     */
    public function testSelectWithParamBoolInWhere(): void
    {
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (1, 'Alice', 1, 0)");
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (2, 'Bob', 0, 1)");

        $stmt = $this->pdo->prepare('SELECT name FROM bool_t WHERE active = ?');
        $stmt->bindValue(1, true, PDO::PARAM_BOOL);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * UPDATE with PARAM_BOOL in SET.
     */
    public function testUpdateWithParamBoolInSet(): void
    {
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (1, 'Alice', 1, 0)");

        $stmt = $this->pdo->prepare('UPDATE bool_t SET active = ?, verified = ? WHERE id = ?');
        $stmt->bindValue(1, false, PDO::PARAM_BOOL);
        $stmt->bindValue(2, true, PDO::PARAM_BOOL);
        $stmt->bindValue(3, 1, PDO::PARAM_INT);
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT active, verified FROM bool_t WHERE id = 1');
        $this->assertEquals(0, (int) $rows[0]['active']);
        $this->assertEquals(1, (int) $rows[0]['verified']);
    }

    /**
     * Boolean PHP value (not PARAM_BOOL type hint) through execute().
     */
    public function testBooleanValueThroughExecute(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bool_t (id, name, active, verified) VALUES (?, ?, ?, ?)');
        $stmt->execute([1, 'Alice', true, false]);

        $rows = $this->ztdQuery('SELECT active, verified FROM bool_t WHERE id = 1');
        $this->assertCount(1, $rows);
        // PHP true/false should be coerced to 1/0
        $this->assertContains((int) $rows[0]['active'], [1, true]);
        $this->assertContains((int) $rows[0]['verified'], [0, false]);
    }

    /**
     * DELETE with PARAM_BOOL in WHERE.
     */
    public function testDeleteWithParamBoolInWhere(): void
    {
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (1, 'Alice', 1, 0)");
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (2, 'Bob', 0, 1)");

        $stmt = $this->pdo->prepare('DELETE FROM bool_t WHERE active = ?');
        $stmt->bindValue(1, false, PDO::PARAM_BOOL);
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT name FROM bool_t');
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Mixed PARAM_BOOL and PARAM_INT in same statement.
     */
    public function testMixedBoolAndIntParams(): void
    {
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (1, 'Alice', 1, 1)");
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (2, 'Bob', 1, 0)");
        $this->pdo->exec("INSERT INTO bool_t (id, name, active, verified) VALUES (3, 'Charlie', 0, 1)");

        $stmt = $this->pdo->prepare('SELECT name FROM bool_t WHERE active = ? AND id > ?');
        $stmt->bindValue(1, true, PDO::PARAM_BOOL);
        $stmt->bindValue(2, 1, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }
}
