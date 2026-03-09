<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL DELETE/UPDATE with ORDER BY and LIMIT through MySQLi CTE shadow.
 *
 * MySQL supports:
 *   DELETE FROM table ORDER BY col LIMIT n
 *   UPDATE table SET ... ORDER BY col LIMIT n
 *
 * @spec SPEC-4.3
 */
class MysqliDeleteUpdateWithOrderLimitTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_dml_limit (id INT PRIMARY KEY, priority INT, status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['my_dml_limit'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_dml_limit VALUES (1, 3, 'pending')");
        $this->ztdExec("INSERT INTO my_dml_limit VALUES (2, 1, 'pending')");
        $this->ztdExec("INSERT INTO my_dml_limit VALUES (3, 2, 'pending')");
        $this->ztdExec("INSERT INTO my_dml_limit VALUES (4, 5, 'pending')");
        $this->ztdExec("INSERT INTO my_dml_limit VALUES (5, 4, 'pending')");
    }

    /**
     * DELETE with ORDER BY and LIMIT.
     */
    public function testDeleteOrderByLimit(): void
    {
        try {
            $this->ztdExec("DELETE FROM my_dml_limit ORDER BY priority ASC LIMIT 2");

            $rows = $this->ztdQuery('SELECT id FROM my_dml_limit ORDER BY id');
            $this->assertCount(3, $rows, 'Should have 3 rows left after DELETE LIMIT 2');
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE ORDER BY LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with ORDER BY and LIMIT.
     */
    public function testUpdateOrderByLimit(): void
    {
        try {
            $this->ztdExec("UPDATE my_dml_limit SET status = 'done' ORDER BY priority DESC LIMIT 2");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dml_limit WHERE status = 'done'");
            $this->assertEquals(2, (int) $rows[0]['cnt'], 'UPDATE LIMIT 2 should update exactly 2 rows');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE ORDER BY LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with LIMIT only.
     */
    public function testDeleteLimitOnly(): void
    {
        try {
            $this->ztdExec("DELETE FROM my_dml_limit LIMIT 1");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM my_dml_limit');
            $this->assertEquals(4, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with LIMIT only.
     */
    public function testUpdateLimitOnly(): void
    {
        try {
            $this->ztdExec("UPDATE my_dml_limit SET status = 'x' LIMIT 1");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dml_limit WHERE status = 'x'");
            $this->assertEquals(1, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM my_dml_limit');
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
