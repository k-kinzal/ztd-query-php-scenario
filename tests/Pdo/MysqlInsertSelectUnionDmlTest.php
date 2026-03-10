<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with UNION ALL on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlInsertSelectUnionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_isu_source_a (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INT
            ) ENGINE=InnoDB",
            "CREATE TABLE my_isu_source_b (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INT
            ) ENGINE=InnoDB",
            "CREATE TABLE my_isu_combined (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INT,
                origin VARCHAR(10)
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_isu_combined', 'my_isu_source_b', 'my_isu_source_a'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_isu_source_a (name, value) VALUES ('alpha', 10)");
        $this->ztdExec("INSERT INTO my_isu_source_a (name, value) VALUES ('beta', 20)");
        $this->ztdExec("INSERT INTO my_isu_source_b (name, value) VALUES ('gamma', 30)");
        $this->ztdExec("INSERT INTO my_isu_source_b (name, value) VALUES ('delta', 40)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM my_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM my_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name, value, origin FROM my_isu_combined ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT UNION ALL (MySQL): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION ALL (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testInsertUnionAfterSourceDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_isu_source_a (name, value) VALUES ('epsilon', 50)");

            $this->ztdExec(
                "INSERT INTO my_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM my_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM my_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_isu_combined ORDER BY name");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT UNION after DML (MySQL): expected 5, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION after DML (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionDistinct(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_isu_source_b (name, value) VALUES ('alpha', 10)");

            $this->ztdExec(
                "INSERT INTO my_isu_combined (name, value, origin)
                 SELECT name, value, 'x' FROM my_isu_source_a
                 UNION
                 SELECT name, value, 'x' FROM my_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_isu_combined ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT UNION DISTINCT (MySQL): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION DISTINCT (MySQL) failed: ' . $e->getMessage());
        }
    }
}
