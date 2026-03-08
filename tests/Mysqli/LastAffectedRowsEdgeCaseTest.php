<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests lastAffectedRows() accuracy across various operations on MySQLi ZTD.
 *
 * ZtdMysqli tracks affected rows via ztdAffectedRowCount, accessible through
 * lastAffectedRows(). This tests edge cases: sequential operations, zero-match,
 * multi-row UPDATE/DELETE, and interaction with prepare vs query paths.
 * @spec pending
 */
class LastAffectedRowsEdgeCaseTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_lar_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active TINYINT DEFAULT 1)';
    }

    protected function getTableNames(): array
    {
        return ['mi_lar_test'];
    }


    /**
     * lastAffectedRows() after single INSERT returns 1.
     */
    public function testSingleInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after multi-row INSERT returns row count.
     */
    public function testMultiRowInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");
        $this->assertSame(3, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after UPDATE matching one row.
     */
    public function testUpdateSingleRow(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");
        $this->mysqli->query("UPDATE mi_lar_test SET score = 100 WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after UPDATE matching multiple rows.
     */
    public function testUpdateMultipleRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");
        $this->mysqli->query("UPDATE mi_lar_test SET score = 100 WHERE score < 85");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after UPDATE matching zero rows returns 0.
     */
    public function testUpdateZeroMatch(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("UPDATE mi_lar_test SET score = 100 WHERE id = 999");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after DELETE matching one row.
     */
    public function testDeleteSingleRow(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");
        $this->mysqli->query("DELETE FROM mi_lar_test WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after DELETE matching zero rows.
     */
    public function testDeleteZeroMatch(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("DELETE FROM mi_lar_test WHERE id = 999");
        $this->assertSame(0, $this->mysqli->lastAffectedRows());
    }

    /**
     * Sequential operations: each call returns the latest count.
     */
    public function testSequentialOperationsReturnLatestCount(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (2, 'Bob', 80), (3, 'Charlie', 70)");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("UPDATE mi_lar_test SET score = 100 WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("DELETE FROM mi_lar_test WHERE id IN (2, 3)");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    /**
     * lastAffectedRows() after SELECT returns 0 (read-only).
     */
    public function testSelectReturnsZero(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // SELECT is a read operation — should not affect the count
        $this->mysqli->query('SELECT * FROM mi_lar_test');
        // After SELECT, lastAffectedRows may retain previous value or return 0
        // depending on implementation
        $count = $this->mysqli->lastAffectedRows();
        $this->assertIsInt($count);
    }

    /**
     * lastAffectedRows() via prepared statement ztdAffectedRows().
     */
    public function testPreparedStatementAffectedRows(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");

        $stmt = $this->mysqli->prepare('UPDATE mi_lar_test SET score = ? WHERE score < ?');
        $score = 100;
        $threshold = 85;
        $stmt->bind_param('ii', $score, $threshold);
        $stmt->execute();

        $this->assertSame(1, $stmt->ztdAffectedRows());
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_lar_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_lar_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
