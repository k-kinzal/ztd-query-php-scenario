<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests that re-executing a prepared SELECT reflects shadow store mutations (MySQLi).
 *
 * Cross-platform verification of Issue #87: prepared SELECT re-execution
 * returns stale shadow data after intervening DML mutations.
 *
 * @spec SPEC-3.2
 */
class MysqliPreparedSelectReexecuteStaleTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE reexec_mi (id INT PRIMARY KEY, val VARCHAR(100), score INT) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['reexec_mi'];
    }

    /**
     * Baseline: fresh query() sees UPDATE mutations.
     */
    public function testFreshQuerySeesUpdateMutation(): void
    {
        $this->mysqli->query("INSERT INTO reexec_mi (id, val, score) VALUES (1, 'original', 10)");
        $this->mysqli->query("UPDATE reexec_mi SET val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT val FROM reexec_mi WHERE id = 1');
        $this->assertSame('updated', $rows[0]['val']);
    }

    /**
     * Prepared SELECT re-execute after UPDATE should see mutation.
     */
    public function testPreparedSelectReexecuteAfterUpdate(): void
    {
        $this->mysqli->query("INSERT INTO reexec_mi (id, val, score) VALUES (1, 'original', 10)");

        $stmt = $this->mysqli->prepare('SELECT val FROM reexec_mi WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('original', $row['val']);
        $stmt->free_result();

        $this->mysqli->query("UPDATE reexec_mi SET val = 'updated' WHERE id = 1");

        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after INSERT should find new row.
     */
    public function testPreparedSelectReexecuteAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO reexec_mi (id, val, score) VALUES (1, 'first', 10)");

        $stmt = $this->mysqli->prepare('SELECT val FROM reexec_mi WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('first', $row['val']);
        $stmt->free_result();

        $this->mysqli->query("INSERT INTO reexec_mi (id, val, score) VALUES (2, 'second', 20)");

        $id = 2;
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertNotNull($row, 'Re-executed prepared SELECT should find newly inserted row');
        $this->assertSame('second', $row['val']);
    }

    /**
     * Prepared SELECT re-execute after DELETE should reflect deletion.
     */
    public function testPreparedSelectReexecuteAfterDelete(): void
    {
        $this->mysqli->query("INSERT INTO reexec_mi (id, val, score) VALUES (1, 'a', 10), (2, 'b', 20)");

        $stmt = $this->mysqli->prepare('SELECT COUNT(*) AS cnt FROM reexec_mi');
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertEquals(2, (int) $row['cnt']);
        $stmt->free_result();

        $this->mysqli->query("DELETE FROM reexec_mi WHERE id = 1");

        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertEquals(1, (int) $row['cnt'], 'Re-executed prepared SELECT should reflect deletion');
    }
}
