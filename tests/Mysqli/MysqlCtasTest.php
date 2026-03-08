<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CREATE TABLE AS SELECT (CTAS) on MySQL MySQLi.
 *
 * MySQL CTAS should work fully — created table is queryable
 * and populated with shadow data.
 * @spec SPEC-5.1c
 */
class MysqlCtasTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ctas_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_ctas_copy AS SELECT * FROM mi_ctas_source',
            'CREATE TABLE mi_ctas_filtered AS SELECT * FROM mi_ctas_source WHERE score >= 85',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ctas_copy', 'mi_ctas_filtered', 'mi_ctas_source', 'AS'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ctas_source VALUES (1, 'Alice', 95)");
        $this->mysqli->query("INSERT INTO mi_ctas_source VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO mi_ctas_source VALUES (3, 'Charlie', 75)");
    }

    /**
     * CTAS copies all shadow data.
     */
    public function testCtasCopiesData(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ctas_copy AS SELECT * FROM mi_ctas_source');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ctas_copy');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_ctas_copy WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * CTAS with filter.
     */
    public function testCtasWithFilter(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ctas_filtered AS SELECT * FROM mi_ctas_source WHERE score >= 85');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ctas_filtered');
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT into CTAS-created table.
     */
    public function testInsertIntoCtasTable(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ctas_copy AS SELECT * FROM mi_ctas_source');
        $this->mysqli->query("INSERT INTO mi_ctas_copy VALUES (4, 'Diana', 80)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ctas_copy');
        $this->assertEquals(4, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ctas_source');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
