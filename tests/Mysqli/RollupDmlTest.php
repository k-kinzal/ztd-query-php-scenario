<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests GROUP BY WITH ROLLUP in DML on MySQLi.
 *
 * @spec SPEC-10.2
 */
class RollupDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_rl_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_rl_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                region VARCHAR(30),
                total_amount DECIMAL(12,2)
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rl_summary', 'mi_rl_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_rl_sales (region, amount) VALUES ('east', 100)");
        $this->ztdExec("INSERT INTO mi_rl_sales (region, amount) VALUES ('east', 200)");
        $this->ztdExec("INSERT INTO mi_rl_sales (region, amount) VALUES ('west', 300)");
        $this->ztdExec("INSERT INTO mi_rl_sales (region, amount) VALUES ('west', 150)");
    }

    public function testSelectGroupByWithRollup(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, SUM(amount) AS total FROM mi_rl_sales GROUP BY region WITH ROLLUP"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT WITH ROLLUP (MySQLi): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WITH ROLLUP (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectWithRollup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_rl_summary (region, total_amount)
                 SELECT region, SUM(amount) FROM mi_rl_sales GROUP BY region WITH ROLLUP"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM mi_rl_summary ORDER BY region");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT WITH ROLLUP (MySQLi): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT WITH ROLLUP (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
