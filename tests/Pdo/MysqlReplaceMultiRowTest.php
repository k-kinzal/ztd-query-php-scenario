<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests REPLACE INTO with multiple rows in a single statement on MySQL PDO.
 *
 * ReplaceMutation::apply() processes each new row by:
 * 1. Filtering out existing rows that match on primary keys
 * 2. Inserting all new rows
 *
 * This test verifies that multi-row REPLACE works correctly via PDO.
 * @spec SPEC-4.2b
 */
class MysqlReplaceMultiRowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_rmr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_rmr_test'];
    }


    /**
     * Multi-row REPLACE inserts all rows when none exist.
     */
    public function testMultiRowReplaceInsertAll(): void
    {
        $this->pdo->exec("REPLACE INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rmr_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Multi-row REPLACE replaces existing rows.
     */
    public function testMultiRowReplaceReplacesExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_rmr_test (id, name, score) VALUES (2, 'Bob', 80)");

        // Replace both existing + add new
        $this->pdo->exec("REPLACE INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice_New', 95), (2, 'Bob_New', 85), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rmr_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pdo_rmr_test WHERE id = 1');
        $this->assertSame('Alice_New', $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pdo_rmr_test WHERE id = 2');
        $this->assertSame('Bob_New', $stmt->fetchColumn());
    }

    /**
     * Multi-row REPLACE with partial overlap.
     */
    public function testMultiRowReplacePartialOverlap(): void
    {
        $this->pdo->exec("INSERT INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_rmr_test (id, name, score) VALUES (3, 'Charlie', 70)");

        $this->pdo->exec("REPLACE INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice_v2', 100), (2, 'Bob', 80), (3, 'Charlie_v2', 75)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rmr_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT score FROM pdo_rmr_test WHERE id = 1');
        $this->assertSame(100, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT score FROM pdo_rmr_test WHERE id = 3');
        $this->assertSame(75, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation: multi-row REPLACE stays in shadow.
     */
    public function testMultiRowReplacePhysicalIsolation(): void
    {
        $this->pdo->exec("REPLACE INTO pdo_rmr_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rmr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
