<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Scenarios\PreparedStatementScenario;
use Tests\Support\AbstractMysqliTestCase;

class PreparedStatementTest extends AbstractMysqliTestCase
{
    use PreparedStatementScenario;

    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE prep_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['prep_test'];
    }

    public function testPreparedSelectWithGetResult(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->mysqli->prepare('SELECT * FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testPreparedUpdateAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $stmt = $this->mysqli->prepare('UPDATE prep_test SET score = ? WHERE score < ?');
        $newScore = 0;
        $threshold = 90;
        $stmt->bind_param('ii', $newScore, $threshold);
        $stmt->execute();

        // Use ztdAffectedRows() for ZTD-aware affected rows
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testPreparedDeleteAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->mysqli->prepare('DELETE FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());

        // Verify deletion in shadow store
        $result = $this->mysqli->query('SELECT * FROM prep_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testExecuteQueryMethod(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        // execute_query is available in PHP 8.2+
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $result = $this->mysqli->execute_query(
            'SELECT * FROM prep_test WHERE id = ?',
            [1]
        );
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testExecuteQueryInsert(): void
    {
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }

        $this->mysqli->execute_query(
            'INSERT INTO prep_test (id, name, score) VALUES (?, ?, ?)',
            [1, 'Alice', 100]
        );

        $result = $this->mysqli->query('SELECT * FROM prep_test WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('Alice', $row['name']);
    }

    public function testQueryRewrittenAtPrepareTime(): void
    {
        $this->mysqli->query("INSERT INTO prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Prepare with ZTD enabled - query is rewritten at prepare time
        $stmt = $this->mysqli->prepare('SELECT * FROM prep_test WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);

        // Even if ZTD is disabled before execute, the prepared query still uses the CTE rewrite
        $this->mysqli->disableZtd();
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        // The query was rewritten at prepare time, so shadow data is still visible
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $this->mysqli->enableZtd();
    }
}
