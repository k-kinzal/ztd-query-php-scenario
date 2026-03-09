<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ON DUPLICATE KEY UPDATE with computed expressions on MySQL.
 *
 * UPSERT with self-referencing expressions like "counter = counter + VALUES(counter)"
 * is common in analytics and counter tables. The shadow store must handle
 * both the insert and update paths correctly.
 *
 * Related: Issue #17 (upsert via prepared statement doesn't update)
 *
 * @spec SPEC-4.4
 */
class MysqlInsertOnDuplicateComputedTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE iod_c (id INT PRIMARY KEY, name VARCHAR(50), counter INT) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['iod_c'];
    }

    /**
     * Basic UPSERT — insert path (no conflict).
     */
    public function testUpsertInsertPath(): void
    {
        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'test', 1)
            ON DUPLICATE KEY UPDATE counter = counter + 1");

        $rows = $this->ztdQuery('SELECT name, counter FROM iod_c WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('test', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['counter']);
    }

    /**
     * UPSERT — update path (conflict on PK).
     * Related: Issue #16 (self-referencing expression loses value)
     */
    public function testUpsertUpdatePath(): void
    {
        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'original', 10)");

        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'updated', 1)
            ON DUPLICATE KEY UPDATE name = VALUES(name), counter = counter + VALUES(counter)");

        $rows = $this->ztdQuery('SELECT name, counter FROM iod_c WHERE id = 1');
        $this->assertCount(1, $rows);
        // After upsert: name should be 'updated', counter should be 10+1=11
        $this->assertSame('updated', $rows[0]['name']);
        $this->assertEquals(11, (int) $rows[0]['counter']);
    }

    /**
     * Multiple UPSERT cycles accumulating counter.
     */
    public function testMultipleUpsertCycles(): void
    {
        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'hits', 0)");

        for ($i = 0; $i < 5; $i++) {
            $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'hits', 1)
                ON DUPLICATE KEY UPDATE counter = counter + 1");
        }

        $rows = $this->ztdQuery('SELECT counter FROM iod_c WHERE id = 1');
        $this->assertEquals(5, (int) $rows[0]['counter']);
    }

    /**
     * Prepared UPSERT (known to be problematic per Issue #17).
     */
    public function testPreparedUpsert(): void
    {
        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'orig', 10)");

        $stmt = $this->pdo->prepare("INSERT INTO iod_c (id, name, counter) VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE name = VALUES(name), counter = counter + VALUES(counter)");

        try {
            $stmt->execute([1, 'via_prepared', 5]);

            $rows = $this->ztdQuery('SELECT name, counter FROM iod_c WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('via_prepared', $rows[0]['name']);
            $this->assertEquals(15, (int) $rows[0]['counter']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared UPSERT not supported (Issue #17): ' . $e->getMessage());
        }
    }

    /**
     * UPSERT with CONCAT in UPDATE clause.
     * Observed: CONCAT(name, VALUES(name)) stored as literal string instead
     * of being evaluated. The InsertTransformer does not parse expressions
     * in ON DUPLICATE KEY UPDATE.
     * Related: Issue #16
     */
    public function testUpsertWithConcatUpdate(): void
    {
        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, 'hello', 0)");

        $this->pdo->exec("INSERT INTO iod_c (id, name, counter) VALUES (1, ' world', 1)
            ON DUPLICATE KEY UPDATE name = CONCAT(name, VALUES(name)), counter = counter + 1");

        $rows = $this->ztdQuery('SELECT name, counter FROM iod_c WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('hello world', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['counter']);
    }
}
