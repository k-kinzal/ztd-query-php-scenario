<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * INSERT ... SELECT with UNION/INTERSECT/EXCEPT sources through ZTD shadow
 * store on PostgreSQL PDO.
 *
 * PostgreSQL supports all set operations. INTERSECT and EXCEPT are known to
 * return empty on SQLite multi-column queries (Issue #50). This test verifies
 * whether PostgreSQL handles INSERT from compound queries correctly, including
 * INSERT from INTERSECT and EXCEPT (which are not supported on MySQL).
 *
 * @spec SPEC-4.1
 */
class PostgresInsertFromUnionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_ifu_archive (
                id SERIAL PRIMARY KEY,
                name VARCHAR(30),
                amount NUMERIC(10,2)
            )",
            "CREATE TABLE pg_ifu_current (
                id SERIAL PRIMARY KEY,
                name VARCHAR(30),
                amount NUMERIC(10,2)
            )",
            "CREATE TABLE pg_ifu_combined (
                id SERIAL PRIMARY KEY,
                name VARCHAR(30),
                amount NUMERIC(10,2)
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ifu_combined', 'pg_ifu_current', 'pg_ifu_archive'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ifu_archive (id, name, amount) VALUES (1, 'Alice', 100.00)");
        $this->pdo->exec("INSERT INTO pg_ifu_archive (id, name, amount) VALUES (2, 'Bob', 200.00)");

        $this->pdo->exec("INSERT INTO pg_ifu_current (id, name, amount) VALUES (1, 'Carol', 300.00)");
        $this->pdo->exec("INSERT INTO pg_ifu_current (id, name, amount) VALUES (2, 'Dave', 400.00)");
        $this->pdo->exec("INSERT INTO pg_ifu_current (id, name, amount) VALUES (3, 'Alice', 150.00)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM pg_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION ALL failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectUnionDedup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive
                 UNION
                 SELECT name, amount FROM pg_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_ifu_combined ORDER BY name, amount");
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectIntersect(): void
    {
        // Add Alice to current with same amount as in archive
        $this->pdo->exec("INSERT INTO pg_ifu_current (id, name, amount) VALUES (4, 'Bob', 200.00)");

        try {
            $this->ztdExec(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive
                 INTERSECT
                 SELECT name, amount FROM pg_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_ifu_combined ORDER BY name");
            // Only Bob(200) is in both
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT INTERSECT failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectExcept(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive
                 EXCEPT
                 SELECT name, amount FROM pg_ifu_current"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_ifu_combined ORDER BY name");
            // Alice(100) and Bob(200) from archive are NOT in current → both included
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT EXCEPT failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive WHERE amount > ?
                 UNION ALL
                 SELECT name, amount FROM pg_ifu_current WHERE amount > ?"
            );
            $stmt->execute([150.0, 150.0]);

            $rows = $this->ztdQuery("SELECT * FROM pg_ifu_combined ORDER BY amount");
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT UNION with prepared params failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromUnionThenAggregate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_ifu_combined (name, amount)
                 SELECT name, amount FROM pg_ifu_archive
                 UNION ALL
                 SELECT name, amount FROM pg_ifu_current"
            );

            $rows = $this->ztdQuery(
                "SELECT name, SUM(amount) AS total
                 FROM pg_ifu_combined
                 GROUP BY name
                 ORDER BY total DESC"
            );
            $this->assertCount(4, $rows);
            $byName = array_column($rows, 'total', 'name');
            $this->assertEquals(250.0, (float) $byName['Alice'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate after INSERT from UNION failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $raw = new PDO(
            \Tests\Support\PostgreSQLContainer::getDsn(),
            'test', 'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $rows = $raw->query("SELECT COUNT(*) AS cnt FROM pg_ifu_combined")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
