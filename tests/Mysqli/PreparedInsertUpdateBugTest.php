<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Confirms prepared INSERT + UPDATE bug on MySQLi (issue #23).
 *
 * Cross-platform parity with MysqlPreparedInsertUpdateBugTest (PDO).
 * @spec pending
 */
class PreparedInsertUpdateBugTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_prep_ins_bug (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_prep_ins_bug'];
    }


    public function testExecInsertThenUpdateWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_prep_ins_bug VALUES (1, 'Alice', 100)");
        $this->mysqli->query('UPDATE mi_prep_ins_bug SET score = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins_bug WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['score']);
    }

    /**
     * Prepared INSERT + query UPDATE works correctly on MySQLi.
     *
     * Unlike PDO where this is a bug (issue #23, update doesn't take effect),
     * MySQLi correctly applies the UPDATE after a prepared INSERT.
     */
    public function testPreparedInsertThenUpdateWorks(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_prep_ins_bug (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 100;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        $this->mysqli->query('UPDATE mi_prep_ins_bug SET score = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT score FROM mi_prep_ins_bug WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['score']);
    }
}
