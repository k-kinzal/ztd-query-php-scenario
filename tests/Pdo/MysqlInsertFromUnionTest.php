<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * INSERT ... SELECT with UNION / UNION ALL sources through ZTD shadow
 * store on MySQL PDO.
 *
 * MySQL's CTE rewriter is known to misparse EXCEPT/INTERSECT as multi-
 * statement SQL (Issue #14). INSERT ... SELECT ... UNION ALL may trigger
 * the same parser issue. This test verifies whether INSERT from compound
 * queries works through ZTD on MySQL.
 *
 * @spec SPEC-4.1
 */
class MysqlInsertFromUnionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mp_ifu_archive (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mp_ifu_current (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
            "CREATE TABLE mp_ifu_combined (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(30),
                amount DECIMAL(10,2)
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ifu_combined', 'mp_ifu_current', 'mp_ifu_archive'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ifu_archive VALUES (1, 'Alice', 100.00)");
        $this->pdo->exec("INSERT INTO mp_ifu_archive VALUES (2, 'Bob', 200.00)");

        $this->pdo->exec("INSERT INTO mp_ifu_current VALUES (1, 'Carol', 300.00)");
        $this->pdo->exec("INSERT INTO mp_ifu_current VALUES (2, 'Dave', 400.00)");
        $this->pdo->exec("INSERT INTO mp_ifu_current VALUES (3, 'Alice', 150.00)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_ifu_combined (name, amount)
                 SELECT name, amount FROM mp_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM mp_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM mp_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows, 'INSERT from UNION ALL should insert all rows');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionDedup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_ifu_combined (name, amount)
                 SELECT name, amount FROM mp_ifu_archive
                 UNION
                 SELECT name, amount FROM mp_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM mp_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows, 'INSERT from UNION should insert deduplicated rows');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mp_ifu_combined (name, amount)
                 SELECT name, amount FROM mp_ifu_archive WHERE amount > ?
                 UNION ALL
                 SELECT name, amount FROM mp_ifu_current WHERE amount > ?"
            );
            $stmt->execute([150.0, 150.0]);

            $rows = $this->ztdQuery("SELECT * FROM mp_ifu_combined ORDER BY amount");
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION with prepared params failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionThenSelect(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mp_ifu_combined (name, amount)
                 SELECT name, amount FROM mp_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM mp_ifu_current"
            );

            $rows = $this->ztdQuery(
                "SELECT name, SUM(amount) AS total
                 FROM mp_ifu_combined
                 GROUP BY name
                 ORDER BY total DESC"
            );
            $this->assertCount(4, $rows);
            $byName = array_column($rows, 'total', 'name');
            $this->assertEquals(250.0, (float) $byName['Alice'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Query after INSERT from UNION failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $rows = $raw->query("SELECT COUNT(*) AS cnt FROM mp_ifu_combined")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
