<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests REPLACE INTO ... SELECT on MySQL PDO ZTD.
 *
 * MySQL supports REPLACE INTO ... SELECT to replace/insert rows from a SELECT.
 * The ReplaceTransformer handles this via $statement->select->build().
 * @spec SPEC-4.1
 */
class MysqlReplaceSelectTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_rsel_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE pdo_rsel_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_rsel_target', 'pdo_rsel_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_rsel_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_rsel_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * REPLACE INTO ... SELECT — all new rows.
     */
    public function testReplaceSelectAllNew(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * REPLACE INTO ... SELECT — with existing rows.
     */
    public function testReplaceSelectWithConflict(): void
    {
        $this->pdo->exec("INSERT INTO pdo_rsel_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name, score FROM pdo_rsel_target WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * REPLACE INTO ... SELECT with WHERE filter.
     */
    public function testReplaceSelectWithWhere(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source WHERE score >= 80');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
