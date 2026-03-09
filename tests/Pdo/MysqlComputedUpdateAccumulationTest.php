<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with self-referencing computed SET expressions on MySQL.
 *
 * Patterns like "SET balance = balance + 100" require the UPDATE rewriter to
 * read the current shadow value and compute the new value. This is commonly
 * used in financial, inventory, and counter applications.
 *
 * @spec SPEC-4.3
 */
class MysqlComputedUpdateAccumulationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE cua (id INT PRIMARY KEY, counter INT, label VARCHAR(50)) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['cua'];
    }

    /**
     * SET counter = counter + 1 accumulates across multiple UPDATEs.
     */
    public function testCounterIncrement(): void
    {
        $this->pdo->exec("INSERT INTO cua (id, counter, label) VALUES (1, 0, 'hits')");

        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec("UPDATE cua SET counter = counter + 1 WHERE id = 1");
        }

        $rows = $this->ztdQuery('SELECT counter FROM cua WHERE id = 1');
        $this->assertEquals(5, (int) $rows[0]['counter']);
    }

    /**
     * SET with multiplication.
     */
    public function testMultiplicationUpdate(): void
    {
        $this->pdo->exec("INSERT INTO cua (id, counter, label) VALUES (1, 2, 'double')");

        $this->pdo->exec("UPDATE cua SET counter = counter * 3 WHERE id = 1");
        $this->pdo->exec("UPDATE cua SET counter = counter + 1 WHERE id = 1");

        $rows = $this->ztdQuery('SELECT counter FROM cua WHERE id = 1');
        $this->assertEquals(7, (int) $rows[0]['counter']); // 2*3=6, 6+1=7
    }

    /**
     * SET with CONCAT on string column.
     */
    public function testConcatUpdate(): void
    {
        $this->pdo->exec("INSERT INTO cua (id, counter, label) VALUES (1, 0, 'hello')");

        $this->pdo->exec("UPDATE cua SET label = CONCAT(label, ' world') WHERE id = 1");

        $rows = $this->ztdQuery('SELECT label FROM cua WHERE id = 1');
        $this->assertSame('hello world', $rows[0]['label']);
    }

    /**
     * Prepared UPDATE with computed SET.
     */
    public function testPreparedComputedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO cua (id, counter, label) VALUES (1, 100, 'test')");

        $stmt = $this->pdo->prepare('UPDATE cua SET counter = counter + ? WHERE id = ?');
        $stmt->execute([50, 1]);
        $stmt->execute([25, 1]);

        $rows = $this->ztdQuery('SELECT counter FROM cua WHERE id = 1');
        $this->assertEquals(175, (int) $rows[0]['counter']); // 100+50+25
    }

    /**
     * Multiple rows updated with computed SET affecting different rows.
     */
    public function testMultiRowComputedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO cua (id, counter, label) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");

        $this->pdo->exec("UPDATE cua SET counter = counter * 2 WHERE counter >= 20");

        $rows = $this->ztdQuery('SELECT id, counter FROM cua ORDER BY id');
        $this->assertEquals(10, (int) $rows[0]['counter']); // unchanged
        $this->assertEquals(40, (int) $rows[1]['counter']); // 20*2
        $this->assertEquals(60, (int) $rows[2]['counter']); // 30*2
    }
}
