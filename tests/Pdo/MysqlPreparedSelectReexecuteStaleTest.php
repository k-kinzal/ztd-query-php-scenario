<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that re-executing a prepared SELECT reflects shadow store mutations (MySQL PDO).
 *
 * Cross-platform verification of Issue #87: prepared SELECT re-execution
 * returns stale shadow data after intervening DML mutations.
 *
 * @spec SPEC-3.2
 */
class MysqlPreparedSelectReexecuteStaleTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE reexec_m (id INT PRIMARY KEY, val VARCHAR(100), score INT) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['reexec_m'];
    }

    /**
     * Baseline: fresh query() sees UPDATE mutations.
     */
    public function testFreshQuerySeesUpdateMutation(): void
    {
        $this->pdo->exec("INSERT INTO reexec_m (id, val, score) VALUES (1, 'original', 10)");
        $this->pdo->exec("UPDATE reexec_m SET val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT val FROM reexec_m WHERE id = 1');
        $this->assertSame('updated', $rows[0]['val']);
    }

    /**
     * Prepared SELECT re-execute after UPDATE should see mutation.
     */
    public function testPreparedSelectReexecuteAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO reexec_m (id, val, score) VALUES (1, 'original', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM reexec_m WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('original', $row['val']);
        $stmt->closeCursor();

        $this->pdo->exec("UPDATE reexec_m SET val = 'updated' WHERE id = 1");

        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after INSERT should find new row.
     */
    public function testPreparedSelectReexecuteAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO reexec_m (id, val, score) VALUES (1, 'first', 10)");

        $stmt = $this->pdo->prepare('SELECT val FROM reexec_m WHERE id = ?');
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('first', $row['val']);
        $stmt->closeCursor();

        $this->pdo->exec("INSERT INTO reexec_m (id, val, score) VALUES (2, 'second', 20)");

        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row, 'Re-executed prepared SELECT should find newly inserted row');
        $this->assertSame('second', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after DELETE should reflect deletion.
     */
    public function testPreparedSelectReexecuteAfterDelete(): void
    {
        $this->pdo->exec("INSERT INTO reexec_m (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM reexec_m');
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(2, (int) $row['cnt']);
        $stmt->closeCursor();

        $this->pdo->exec("DELETE FROM reexec_m WHERE id = 1");

        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(1, (int) $row['cnt'], 'Re-executed prepared SELECT should reflect deletion');
    }
}
