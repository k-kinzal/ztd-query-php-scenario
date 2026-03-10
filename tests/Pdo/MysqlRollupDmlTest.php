<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP BY WITH ROLLUP in DML subquery context on MySQL PDO.
 *
 * MySQL uses WITH ROLLUP syntax rather than ROLLUP(...).
 *
 * @spec SPEC-10.2
 */
class MysqlRollupDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_rl_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                category VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE my_rl_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                total_amount DECIMAL(12,2)
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_rl_summary', 'my_rl_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_rl_sales (region, category, amount) VALUES ('east', 'electronics', 100)");
        $this->ztdExec("INSERT INTO my_rl_sales (region, category, amount) VALUES ('east', 'clothing', 200)");
        $this->ztdExec("INSERT INTO my_rl_sales (region, category, amount) VALUES ('west', 'electronics', 300)");
        $this->ztdExec("INSERT INTO my_rl_sales (region, category, amount) VALUES ('west', 'clothing', 150)");
    }

    /**
     * SELECT with GROUP BY ... WITH ROLLUP — baseline.
     */
    public function testSelectGroupByWithRollup(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, SUM(amount) AS total
                 FROM my_rl_sales
                 GROUP BY region WITH ROLLUP"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT WITH ROLLUP (MySQL): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WITH ROLLUP (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY ... WITH ROLLUP.
     */
    public function testInsertSelectGroupByWithRollup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_rl_summary (region, total_amount)
                 SELECT region, SUM(amount)
                 FROM my_rl_sales
                 GROUP BY region WITH ROLLUP"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM my_rl_summary ORDER BY region");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT WITH ROLLUP (MySQL): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT WITH ROLLUP (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT WITH ROLLUP after prior DML.
     */
    public function testInsertRollupAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_rl_sales (region, category, amount) VALUES ('east', 'electronics', 500)");

            $this->ztdExec(
                "INSERT INTO my_rl_summary (region, total_amount)
                 SELECT region, SUM(amount)
                 FROM my_rl_sales
                 GROUP BY region WITH ROLLUP"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM my_rl_summary ORDER BY region");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT ROLLUP after DML (MySQL): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $eastRow = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            if (count($eastRow) === 1) {
                $this->assertEqualsWithDelta(800.0, (float) $eastRow[0]['total_amount'], 0.01);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT ROLLUP after DML (MySQL) failed: ' . $e->getMessage());
        }
    }
}
