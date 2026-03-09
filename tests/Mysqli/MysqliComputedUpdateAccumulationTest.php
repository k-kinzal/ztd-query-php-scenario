<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with self-referencing computed SET expressions on MySQLi.
 *
 * Verifies counter accumulation, string concatenation, and prepared
 * computed UPDATE patterns through the MySQLi adapter.
 *
 * @spec SPEC-4.3
 */
class MysqliComputedUpdateAccumulationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE cua_mi (id INT PRIMARY KEY, counter INT, label VARCHAR(100)) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['cua_mi'];
    }

    /**
     * SET counter = counter + 1 accumulates across multiple UPDATEs via query().
     */
    public function testCounterIncrementViaQuery(): void
    {
        $this->mysqli->query("INSERT INTO cua_mi (id, counter, label) VALUES (1, 0, 'hits')");

        for ($i = 1; $i <= 5; $i++) {
            $this->mysqli->query("UPDATE cua_mi SET counter = counter + 1 WHERE id = 1");
        }

        $rows = $this->ztdQuery('SELECT counter FROM cua_mi WHERE id = 1');
        $this->assertEquals(5, (int) $rows[0]['counter']);
    }

    /**
     * SET with CONCAT via query().
     */
    public function testConcatUpdateViaQuery(): void
    {
        $this->mysqli->query("INSERT INTO cua_mi (id, counter, label) VALUES (1, 0, 'hello')");

        $this->mysqli->query("UPDATE cua_mi SET label = CONCAT(label, ' world') WHERE id = 1");

        $rows = $this->ztdQuery('SELECT label FROM cua_mi WHERE id = 1');
        $this->assertSame('hello world', $rows[0]['label']);
    }

    /**
     * Prepared UPDATE with computed SET and re-execution.
     */
    public function testPreparedComputedUpdateReexecute(): void
    {
        $this->mysqli->query("INSERT INTO cua_mi (id, counter, label) VALUES (1, 100, 'test')");

        $stmt = $this->mysqli->prepare('UPDATE cua_mi SET counter = counter + ? WHERE id = ?');
        $amount = 50;
        $id = 1;
        $stmt->bind_param('ii', $amount, $id);
        $stmt->execute();

        $amount = 25;
        $stmt->execute();

        $rows = $this->ztdQuery('SELECT counter FROM cua_mi WHERE id = 1');
        $this->assertEquals(175, (int) $rows[0]['counter']); // 100+50+25
    }

    /**
     * MySQLi execute_query() with computed UPDATE (PHP 8.2+).
     */
    public function testExecuteQueryComputedUpdate(): void
    {
        if (!method_exists($this->mysqli, 'execute_query')) {
            $this->markTestSkipped('execute_query() not available (requires PHP 8.2+)');
        }

        $this->mysqli->query("INSERT INTO cua_mi (id, counter, label) VALUES (1, 0, 'test')");

        $this->mysqli->execute_query('UPDATE cua_mi SET counter = counter + ? WHERE id = ?', [10, 1]);
        $this->mysqli->execute_query('UPDATE cua_mi SET counter = counter + ? WHERE id = ?', [5, 1]);

        $rows = $this->ztdQuery('SELECT counter FROM cua_mi WHERE id = 1');
        $this->assertEquals(15, (int) $rows[0]['counter']);
    }

    /**
     * Multi-row UPDATE with computed SET.
     */
    public function testMultiRowComputedUpdate(): void
    {
        $this->mysqli->query("INSERT INTO cua_mi (id, counter, label) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");

        $result = $this->mysqli->query("UPDATE cua_mi SET counter = counter * 2 WHERE counter >= 20");
        $affected = $this->mysqli->lastAffectedRows();
        $this->assertEquals(2, $affected);

        $rows = $this->ztdQuery('SELECT id, counter FROM cua_mi ORDER BY id');
        $this->assertEquals(10, (int) $rows[0]['counter']); // unchanged
        $this->assertEquals(40, (int) $rows[1]['counter']); // 20*2
        $this->assertEquals(60, (int) $rows[2]['counter']); // 30*2
    }
}
