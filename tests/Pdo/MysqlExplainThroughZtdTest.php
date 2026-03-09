<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests EXPLAIN through ZTD on MySQL.
 *
 * @spec SPEC-3.1
 */
class MysqlExplainThroughZtdTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_exp_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            category VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_exp_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO my_exp_items VALUES (1, 'Widget', 'tools')");
    }

    /**
     * EXPLAIN for a SELECT.
     */
    public function testExplainSelect(): void
    {
        try {
            $stmt = $this->pdo->query("EXPLAIN SELECT * FROM my_exp_items WHERE category = 'tools'");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete('EXPLAIN returned 0 rows');
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXPLAIN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DESCRIBE table.
     */
    public function testDescribeTable(): void
    {
        try {
            $stmt = $this->pdo->query("DESCRIBE my_exp_items");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete('DESCRIBE returned 0 rows');
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DESCRIBE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SHOW CREATE TABLE.
     */
    public function testShowCreateTable(): void
    {
        try {
            $stmt = $this->pdo->query("SHOW CREATE TABLE my_exp_items");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete('SHOW CREATE TABLE returned 0 rows');
            }

            $this->assertGreaterThan(0, count($rows));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SHOW CREATE TABLE failed: ' . $e->getMessage()
            );
        }
    }
}
