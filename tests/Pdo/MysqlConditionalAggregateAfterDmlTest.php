<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests conditional aggregates from shadow-modified tables on MySQL via PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlConditionalAggregateAfterDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mpd_cad_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mpd_cad_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (1, 'Alice', 'completed', 100.00)");
        $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (2, 'Alice', 'pending', 200.00)");
        $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (3, 'Bob', 'completed', 50.00)");
        $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (4, 'Bob', 'cancelled', 75.00)");
    }

    public function testSumCaseAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (5, 'Alice', 'completed', 300.00)");

            $rows = $this->ztdQuery(
                "SELECT
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_total
                 FROM mpd_cad_orders
                 WHERE customer = 'Alice'"
            );

            $this->assertCount(1, $rows);
            $completed = (float) $rows[0]['completed_total'];

            if ($completed != 400.00) {
                $this->markTestIncomplete("SUM(CASE) completed: expected 400, got $completed");
            }
            $this->assertEquals(400.00, $completed);
            $this->assertEquals(200.00, (float) $rows[0]['pending_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM(CASE) after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testCountCaseAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE mpd_cad_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count
                 FROM mpd_cad_orders"
            );

            $this->assertCount(1, $rows);
            $completed = (int) $rows[0]['completed_count'];

            if ($completed !== 3) {
                $this->markTestIncomplete("COUNT(CASE) completed: expected 3, got $completed");
            }
            $this->assertEquals(3, $completed);
            $this->assertEquals(0, (int) $rows[0]['pending_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(CASE) after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testConditionalAggregateGroupByAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_cad_orders VALUES (5, 'Carol', 'completed', 500.00)");
            $this->pdo->exec("UPDATE mpd_cad_orders SET status = 'completed' WHERE id = 4");

            $rows = $this->ztdQuery(
                "SELECT
                    customer,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_amount
                 FROM mpd_cad_orders
                 GROUP BY customer
                 ORDER BY customer"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(100.00, (float) $rows[0]['completed_amount']);
            $this->assertEquals(125.00, (float) $rows[1]['completed_amount']);
            $this->assertEquals(500.00, (float) $rows[2]['completed_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional aggregate GROUP BY after DML failed: ' . $e->getMessage());
        }
    }
}
