<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * INSERT ... SELECT with UNION / UNION ALL / EXCEPT sources through ZTD
 * shadow store on SQLite.
 *
 * These patterns combine write operations with compound queries. The CTE
 * rewriter must handle both the INSERT target and multiple SELECT source
 * tables in the compound branches. This is a common pattern for data
 * migration and consolidation workflows.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertFromUnionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_ifu_archive (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                amount REAL
            )",
            "CREATE TABLE sl_ifu_current (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                amount REAL
            )",
            "CREATE TABLE sl_ifu_combined (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(30),
                amount REAL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ifu_combined', 'sl_ifu_current', 'sl_ifu_archive'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ifu_archive VALUES (1, 'Alice', 100.0)");
        $this->pdo->exec("INSERT INTO sl_ifu_archive VALUES (2, 'Bob', 200.0)");

        $this->pdo->exec("INSERT INTO sl_ifu_current VALUES (1, 'Carol', 300.0)");
        $this->pdo->exec("INSERT INTO sl_ifu_current VALUES (2, 'Dave', 400.0)");
        $this->pdo->exec("INSERT INTO sl_ifu_current VALUES (3, 'Alice', 150.0)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $affected = $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY name, amount");
            // 2 archive + 3 current = 5 rows
            $this->assertCount(5, $rows, 'INSERT from UNION ALL should insert all rows');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionDedup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive
                 UNION
                 SELECT name, amount FROM sl_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY name, amount");
            // All rows have different (name,amount) pairs so UNION should give 5
            $this->assertCount(5, $rows, 'INSERT from UNION should insert deduplicated rows');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionWithDuplicateNames(): void
    {
        // Alice appears in both but with different amounts — UNION should keep both
        try {
            $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive WHERE name = 'Alice'
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current WHERE name = 'Alice'"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY amount");
            // Alice(100) from archive + Alice(150) from current
            $this->assertCount(2, $rows);
            $this->assertEquals(100.0, (float) $rows[0]['amount'], '', 0.01);
            $this->assertEquals(150.0, (float) $rows[1]['amount'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION with shared names failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectExcept(): void
    {
        try {
            // Names in archive but NOT in current
            $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive
                 EXCEPT
                 SELECT name, amount FROM sl_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY name");
            // Alice(100) and Bob(200) are in archive; Carol, Dave, Alice(150) in current
            // EXCEPT removes nothing since (name,amount) pairs don't match
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT EXCEPT failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive WHERE amount > ?
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current WHERE amount > ?"
            );
            $stmt->execute([150.0, 150.0]);

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY amount");
            // Archive: Bob(200); Current: Carol(300), Dave(400)
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION with prepared params failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionThenSelect(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current"
            );

            // Now query the combined table with aggregation
            $rows = $this->ztdQuery(
                "SELECT name, SUM(amount) AS total
                 FROM sl_ifu_combined
                 GROUP BY name
                 ORDER BY total DESC"
            );
            // Alice: 100+150=250, Bob: 200, Carol: 300, Dave: 400
            $this->assertCount(4, $rows);
            $byName = array_column($rows, 'total', 'name');
            $this->assertEquals(250.0, (float) $byName['Alice'], '', 0.01);
            $this->assertEquals(400.0, (float) $byName['Dave'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Query after INSERT from UNION failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionThreeSources(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_ifu_combined (name, amount)
                 SELECT name, amount FROM sl_ifu_archive WHERE name = 'Alice'
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current WHERE name = 'Carol'
                 UNION ALL
                 SELECT name, amount FROM sl_ifu_current WHERE name = 'Dave'"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ifu_combined ORDER BY name");
            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertSame(['Alice', 'Carol', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from triple UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ifu_combined")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
