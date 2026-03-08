<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests TRUNCATE TABLE + re-insert workflow on MySQLi ZTD.
 *
 * MySQL supports TRUNCATE TABLE as DDL.
 * After truncating in shadow mode, new INSERTs should work normally.
 * Tests the full lifecycle: insert → truncate → re-insert → verify.
 * @spec SPEC-5.3
 */
class TruncateReinsertWorkflowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_trunc_wf (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_trunc_wf'];
    }


    /**
     * TRUNCATE TABLE then re-insert.
     */
    public function testTruncateThenReinsert(): void
    {
        // Insert initial data
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (2, 'Bob', 80)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trunc_wf');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        // Truncate
        $this->mysqli->query('TRUNCATE TABLE mi_trunc_wf');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trunc_wf');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        // Re-insert
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (10, 'Xavier', 95)");
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (20, 'Yara', 85)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trunc_wf');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_trunc_wf ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Xavier', $rows[0]['name']);
        $this->assertSame('Yara', $rows[1]['name']);
    }

    /**
     * Re-insert with same IDs after TRUNCATE.
     */
    public function testReinsertSameIdsAfterTruncate(): void
    {
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query('TRUNCATE TABLE mi_trunc_wf');

        // Same ID, different data
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (1, 'Alice V2', 95)");

        $result = $this->mysqli->query('SELECT name, score FROM mi_trunc_wf WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * Multiple truncate-reinsert cycles.
     */
    public function testMultipleTruncateReinsertCycles(): void
    {
        for ($cycle = 1; $cycle <= 3; $cycle++) {
            $this->mysqli->query('TRUNCATE TABLE mi_trunc_wf');

            for ($i = 1; $i <= $cycle; $i++) {
                $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES ($i, 'C{$cycle}_U{$i}', " . ($cycle * 10) . ')');
            }

            $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trunc_wf');
            $this->assertSame($cycle, (int) $result->fetch_assoc()['cnt'], "Cycle $cycle should have $cycle rows");
        }
    }

    /**
     * Physical isolation after truncate + reinsert.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query('TRUNCATE TABLE mi_trunc_wf');
        $this->mysqli->query("INSERT INTO mi_trunc_wf (id, name, score) VALUES (2, 'Bob', 80)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_trunc_wf');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
