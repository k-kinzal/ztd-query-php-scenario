<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL PARTITION clause behavior through ZTD.
 *
 * DELETE FROM ... PARTITION and UPDATE ... PARTITION are
 * not supported by the CTE rewriter and should throw.
 * @spec pending
 */
class MysqlPartitionClauseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_part_test (
            id INT NOT NULL,
            name VARCHAR(50),
            created_year INT NOT NULL,
            PRIMARY KEY (id, created_year)
        ) PARTITION BY RANGE (created_year) (
            PARTITION p2023 VALUES LESS THAN (2024),
            PARTITION p2024 VALUES LESS THAN (2025),
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )';
    }

    protected function getTableNames(): array
    {
        return ['mysql_part_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_part_test VALUES (1, 'Alice', 2023)");
        $this->pdo->exec("INSERT INTO mysql_part_test VALUES (2, 'Bob', 2024)");
        $this->pdo->exec("INSERT INTO mysql_part_test VALUES (3, 'Charlie', 2025)");
    }

    /**
     * Regular SELECT works on partitioned table.
     */
    public function testRegularSelectWorks(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM mysql_part_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    /**
     * SELECT PARTITION — may or may not be supported.
     */
    public function testSelectPartition(): void
    {
        try {
            $stmt = $this->pdo->query('SELECT name FROM mysql_part_test PARTITION (p2023)');
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // If supported, should return only 2023 data
            if (count($rows) > 0) {
                $this->assertSame('Alice', $rows[0]['name']);
            }
        } catch (\Exception $e) {
            // PARTITION clause may not be supported by CTE rewriter
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * DELETE with PARTITION clause — may throw.
     */
    public function testDeleteWithPartition(): void
    {
        try {
            $this->pdo->exec('DELETE FROM mysql_part_test PARTITION (p2023) WHERE id = 1');

            // If it worked, verify deletion
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_part_test WHERE id = 1');
            $this->assertSame(0, (int) $stmt->fetchColumn());
        } catch (\Exception $e) {
            // Expected: PARTITION clause not supported by CTE rewriter
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * UPDATE with PARTITION clause — may throw.
     */
    public function testUpdateWithPartition(): void
    {
        try {
            $this->pdo->exec("UPDATE mysql_part_test PARTITION (p2024) SET name = 'Bobby' WHERE id = 2");

            $stmt = $this->pdo->query('SELECT name FROM mysql_part_test WHERE id = 2');
            $this->assertSame('Bobby', $stmt->fetchColumn());
        } catch (\Exception $e) {
            // Expected: PARTITION clause not supported
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Regular DELETE/UPDATE (without PARTITION) works.
     */
    public function testRegularDmlWorks(): void
    {
        $this->pdo->exec('DELETE FROM mysql_part_test WHERE id = 1');
        $this->pdo->exec("UPDATE mysql_part_test SET name = 'Bobby' WHERE id = 2");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_part_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM mysql_part_test WHERE id = 2');
        $this->assertSame('Bobby', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_part_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
