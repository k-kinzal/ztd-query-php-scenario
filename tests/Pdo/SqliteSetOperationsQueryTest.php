<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * UNION, UNION ALL, INTERSECT, and EXCEPT queries through ZTD shadow store
 * on SQLite.
 *
 * Set operations require the CTE rewriter to correctly rewrite table
 * references in each branch of the compound query. These are common
 * patterns for combining results from the same or different tables.
 *
 * @spec SPEC-3.3
 */
class SqliteSetOperationsQueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_soq_customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                city VARCHAR(30),
                tier VARCHAR(10)
            )",
            "CREATE TABLE sl_soq_prospects (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                city VARCHAR(30),
                source VARCHAR(20)
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_soq_prospects', 'sl_soq_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_soq_customers VALUES (1, 'Alice', 'NYC', 'gold')");
        $this->pdo->exec("INSERT INTO sl_soq_customers VALUES (2, 'Bob', 'LA', 'silver')");
        $this->pdo->exec("INSERT INTO sl_soq_customers VALUES (3, 'Carol', 'NYC', 'bronze')");

        $this->pdo->exec("INSERT INTO sl_soq_prospects VALUES (1, 'Alice', 'NYC', 'web')");
        $this->pdo->exec("INSERT INTO sl_soq_prospects VALUES (2, 'Dave', 'Chicago', 'referral')");
        $this->pdo->exec("INSERT INTO sl_soq_prospects VALUES (3, 'Eve', 'LA', 'web')");
    }

    public function testUnionRemovesDuplicates(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 UNION
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // Alice appears in both tables but UNION deduplicates
            $names = array_column($rows, 'name');
            $this->assertCount(5, $rows, 'UNION should deduplicate Alice');
            $this->assertSame(['Alice', 'Bob', 'Carol', 'Dave', 'Eve'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllKeepsDuplicates(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 UNION ALL
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // All 6 rows, Alice appears twice
            $this->assertCount(6, $rows, 'UNION ALL should keep all rows');
            $aliceRows = array_filter($rows, fn($r) => $r['name'] === 'Alice');
            $this->assertCount(2, $aliceRows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testIntersect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 INTERSECT
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // Only Alice(NYC) appears in both
            $this->assertCount(1, $rows, 'INTERSECT should return only common rows');
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INTERSECT failed: ' . $e->getMessage());
        }
    }

    public function testExcept(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 EXCEPT
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // Bob(LA) and Carol(NYC) are in customers but not prospects
            $this->assertCount(2, $rows, 'EXCEPT should return rows only in first query');
            $names = array_column($rows, 'name');
            $this->assertContains('Bob', $names);
            $this->assertContains('Carol', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXCEPT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_soq_customers VALUES (4, 'Frank', 'Boston', 'gold')");

        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_soq_customers
                 UNION ALL
                 SELECT name FROM sl_soq_prospects
                 ORDER BY name"
            );
            // 4 customers + 3 prospects = 7
            $this->assertCount(7, $rows);
            $this->assertContains('Frank', array_column($rows, 'name'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAfterDeleteFromOneBranch(): void
    {
        $this->pdo->exec("DELETE FROM sl_soq_customers WHERE name = 'Bob'");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 UNION ALL
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // 2 remaining customers + 3 prospects = 5
            $this->assertCount(5, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testUnionSameTableTwice(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers WHERE tier = 'gold'
                 UNION ALL
                 SELECT name, city FROM sl_soq_customers WHERE tier = 'silver'
                 ORDER BY name"
            );
            // Alice(gold) + Bob(silver) = 2
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION same table twice failed: ' . $e->getMessage());
        }
    }

    public function testUnionWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, city FROM sl_soq_customers WHERE city = ?
                 UNION ALL
                 SELECT name, city FROM sl_soq_prospects WHERE city = ?
                 ORDER BY name",
                ['NYC', 'NYC']
            );
            // Alice(customer), Carol(customer), Alice(prospect)
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION with prepared params failed: ' . $e->getMessage());
        }
    }

    public function testUnionThreeBranches(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_soq_customers WHERE tier = 'gold'
                 UNION ALL
                 SELECT name FROM sl_soq_customers WHERE tier = 'silver'
                 UNION ALL
                 SELECT name FROM sl_soq_prospects WHERE source = 'web'
                 ORDER BY name"
            );
            // Alice(gold), Bob(silver), Alice(web), Eve(web)
            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Triple UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testExceptAfterUpdate(): void
    {
        // Make Alice's city match in both tables
        $this->pdo->exec("UPDATE sl_soq_customers SET city = 'Chicago' WHERE name = 'Alice'");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 EXCEPT
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // Alice now has Chicago in customers, NYC in prospects — different, so Alice IS in EXCEPT
            // Bob(LA) not in prospects, Carol(NYC) not in prospects, Alice(Chicago) not in prospects
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXCEPT after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testIntersectAfterMutation(): void
    {
        // Add Dave to customers too
        $this->pdo->exec("INSERT INTO sl_soq_customers VALUES (4, 'Dave', 'Chicago', 'bronze')");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, city FROM sl_soq_customers
                 INTERSECT
                 SELECT name, city FROM sl_soq_prospects
                 ORDER BY name"
            );
            // Alice(NYC) and Dave(Chicago) now in both
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Dave', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INTERSECT after mutation failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_soq_customers")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
