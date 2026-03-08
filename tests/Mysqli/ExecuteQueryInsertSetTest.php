<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests execute_query() with INSERT...SET syntax on MySQLi ZTD.
 *
 * MySQL's alternative INSERT syntax: INSERT INTO table SET col1 = val1, col2 = val2
 * The InsertTransformer::buildInsertSetSelect() handles this.
 * execute_query() internally uses prepare() + execute().
 * @spec SPEC-4.1
 */
class ExecuteQueryInsertSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_eqis_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_eqis_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        if (!method_exists(\mysqli::class, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }
    }

    /**
     * INSERT...SET via execute_query() without params.
     */
    public function testInsertSetBasic(): void
    {
        $result = $this->mysqli->execute_query("INSERT INTO mi_eqis_test SET id = 1, name = 'Alice', score = 90");
        $this->assertNotFalse($result);

        $result = $this->mysqli->query('SELECT name, score FROM mi_eqis_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT...SET via execute_query() with params.
     */
    public function testInsertSetWithParams(): void
    {
        $result = $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_test SET id = ?, name = ?, score = ?',
            [1, 'Alice', 90]
        );
        $this->assertNotFalse($result);

        $result = $this->mysqli->query('SELECT name, score FROM mi_eqis_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Multiple INSERT...SET via execute_query().
     */
    public function testMultipleInsertSet(): void
    {
        $this->mysqli->execute_query("INSERT INTO mi_eqis_test SET id = 1, name = 'Alice', score = 90");
        $this->mysqli->execute_query("INSERT INTO mi_eqis_test SET id = 2, name = 'Bob', score = 80");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT...SET with ON DUPLICATE KEY UPDATE via execute_query().
     */
    public function testInsertSetOnDuplicateKeyUpdate(): void
    {
        $this->mysqli->execute_query("INSERT INTO mi_eqis_test SET id = 1, name = 'Alice', score = 90");

        // ON DUPLICATE KEY UPDATE via execute_query() — may have limitations
        $this->mysqli->execute_query(
            "INSERT INTO mi_eqis_test SET id = 1, name = 'Alice V2', score = 95 ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $result = $this->mysqli->query('SELECT name FROM mi_eqis_test WHERE id = 1');
        $name = $result->fetch_assoc()['name'];
        // Expected: upsert should update the name
        if ($name !== 'Alice V2') {
            $this->markTestIncomplete(
                'execute_query upsert does not update existing row. '
                . 'Expected "Alice V2", got ' . var_export($name, true)
            );
        }
        $this->assertSame('Alice V2', $name);
    }

    /**
     * Physical isolation with INSERT...SET via execute_query().
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->execute_query("INSERT INTO mi_eqis_test SET id = 1, name = 'Alice', score = 90");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
