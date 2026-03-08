<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests execute_query() with INSERT...SELECT patterns on MySQLi.
 *
 * execute_query() internally uses prepare() + execute(), so it should
 * support INSERT...SELECT (both cross-table and self-referencing).
 * This covers patterns not tested by ExecuteQueryWriteOpsTest or
 * ExecuteQueryInsertSetTest.
 * @spec pending
 */
class ExecuteQueryInsertSelectTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_eqis_src (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))',
            'CREATE TABLE mi_eqis_dst (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_eqis_src', 'mi_eqis_dst'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        if (!method_exists(\mysqli::class, 'execute_query')) {
            $this->markTestSkipped('execute_query requires PHP 8.2+');
        }
        // Seed source table
        $this->mysqli->query("INSERT INTO mi_eqis_src (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query("INSERT INTO mi_eqis_src (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->mysqli->query("INSERT INTO mi_eqis_src (id, name, score, category) VALUES (3, 'Charlie', 70, 'A')");
    }

    /**
     * INSERT...SELECT cross-table via execute_query().
     */
    public function testCrossTableInsertSelect(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_dst (id, name, score, category) SELECT id, name, score, category FROM mi_eqis_src'
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_dst');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT...SELECT with WHERE filter via execute_query().
     */
    public function testInsertSelectWithFilter(): void
    {
        $this->mysqli->execute_query(
            "INSERT INTO mi_eqis_dst (id, name, score, category) SELECT id, name, score, category FROM mi_eqis_src WHERE category = 'A'"
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_dst');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT...SELECT with params in WHERE via execute_query().
     */
    public function testInsertSelectWithParams(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_dst (id, name, score, category) SELECT id, name, score, category FROM mi_eqis_src WHERE score >= ?',
            [80]
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_dst');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Self-referencing INSERT...SELECT via execute_query().
     */
    public function testSelfReferencingInsertSelect(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_src (id, name, score, category) SELECT id + 100, name, score, category FROM mi_eqis_src'
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_src');
        $this->assertSame(6, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT...SELECT then query the destination via execute_query().
     */
    public function testInsertSelectThenQuery(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_dst (id, name, score, category) SELECT id, name, score, category FROM mi_eqis_src'
        );

        $result = $this->mysqli->execute_query(
            'SELECT name FROM mi_eqis_dst WHERE id = ?',
            [1]
        );
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation after INSERT...SELECT via execute_query().
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->execute_query(
            'INSERT INTO mi_eqis_dst (id, name, score, category) SELECT id, name, score, category FROM mi_eqis_src'
        );

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_eqis_dst');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
