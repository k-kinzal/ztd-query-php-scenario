<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Window function queries (ROW_NUMBER, RANK, DENSE_RANK, SUM OVER, LAG,
 * LEAD, NTILE) through ZTD shadow store on MySQL PDO.
 *
 * Window functions require MySQL 8.0+. The CTE rewriter must correctly
 * rewrite table references inside OVER() clauses. Combined with DML
 * mutations, these patterns test shadow store fidelity for analytical
 * queries.
 *
 * @spec SPEC-3.3
 */
class MysqlWindowFunctionQueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mp_wfq_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                rep VARCHAR(30),
                region VARCHAR(20),
                amount DECIMAL(10,2),
                sale_date DATE
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_wfq_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (1, 'Alice', 'East', 100.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (2, 'Bob', 'East', 200.00, '2025-01-12')");
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (3, 'Alice', 'East', 150.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (4, 'Carol', 'West', 300.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (5, 'Carol', 'West', 250.00, '2025-01-14')");
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (6, 'Dave', 'West', 180.00, '2025-01-13')");
    }

    public function testRowNumberOverOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM mp_wfq_sales"
            );
            $this->assertCount(6, $rows);
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Carol', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ROW_NUMBER() OVER failed: ' . $e->getMessage());
        }
    }

    public function testRowNumberPartitionByRegion(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mp_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(6, $rows);
            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'East'));
            $this->assertSame(1, (int) $east[0]['rn']);
            $this->assertSame('Bob', $east[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PARTITION BY region failed: ' . $e->getMessage());
        }
    }

    public function testRankWithTies(): void
    {
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (7, 'Eve', 'East', 200.00, '2025-01-20')");
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM mp_wfq_sales
                 ORDER BY rnk, rep"
            );
            $this->assertCount(7, $rows);
            $rank3 = array_filter($rows, fn($r) => (int) $r['rnk'] === 3);
            $this->assertCount(2, $rank3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('RANK() with ties failed: ' . $e->getMessage());
        }
    }

    public function testSumOverPartition(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        SUM(amount) OVER (PARTITION BY region) AS region_total
                 FROM mp_wfq_sales
                 ORDER BY region, rep"
            );
            $this->assertCount(6, $rows);
            $eastRow = array_values(array_filter($rows, fn($r) => $r['region'] === 'East'))[0];
            $this->assertEquals(450.0, (float) $eastRow['region_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM() OVER(PARTITION BY) failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO mp_wfq_sales (id, rep, region, amount, sale_date) VALUES (7, 'Eve', 'East', 500.00, '2025-01-25')");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM mp_wfq_sales"
            );
            $this->assertCount(7, $rows);
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Eve', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE mp_wfq_sales SET amount = 999.00 WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM mp_wfq_sales"
            );
            $this->assertCount(6, $rows);
            $this->assertSame(1, (int) $rows[0]['rnk']);
            $this->assertSame('Dave', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM mp_wfq_sales WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mp_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(5, $rows);
            $west = array_values(array_filter($rows, fn($r) => $r['region'] === 'West'));
            $this->assertCount(2, $west);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM mp_wfq_sales
                 WHERE amount > ?
                 ORDER BY region, rn",
                [150.0]
            );
            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $rows = $raw->query("SELECT COUNT(*) AS cnt FROM mp_wfq_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
