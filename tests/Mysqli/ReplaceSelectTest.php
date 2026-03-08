<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests REPLACE INTO ... SELECT on MySQLi ZTD.
 *
 * MySQL supports REPLACE INTO ... SELECT to replace/insert rows from a SELECT.
 * The ReplaceTransformer handles this by building SELECT SQL from the
 * statement's select property, and ReplaceMutation handles the shadow store.
 * @spec pending
 */
class ReplaceSelectTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rsel_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_rsel_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rsel_target', 'mi_rsel_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_rsel_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * REPLACE INTO ... SELECT — all new rows.
     */
    public function testReplaceSelectAllNew(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_rsel_target WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * REPLACE INTO ... SELECT — with existing rows to replace.
     */
    public function testReplaceSelectWithConflict(): void
    {
        $this->mysqli->query("INSERT INTO mi_rsel_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        // id=1 should be replaced
        $result = $this->mysqli->query('SELECT name, score FROM mi_rsel_target WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * REPLACE INTO ... SELECT with WHERE filter.
     */
    public function testReplaceSelectWithWhere(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source WHERE score >= 80');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation: REPLACE INTO ... SELECT stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('REPLACE INTO mi_rsel_target (id, name, score) SELECT id, name, score FROM mi_rsel_source');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rsel_target');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
