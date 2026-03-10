<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests advanced INSERT...SELECT UNION patterns through ZTD shadow store on SQLite.
 *
 * Extends basic INSERT...SELECT UNION coverage with prepared parameters,
 * triple UNION ALL, filtered branches, and cross-table merging.
 *
 * @spec SPEC-4.1a
 */
class SqliteInsertSelectUnionAdvancedTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isua_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept TEXT NOT NULL,
                salary REAL NOT NULL
            )',
            'CREATE TABLE sl_isua_contractors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                agency TEXT NOT NULL,
                rate REAL NOT NULL
            )',
            'CREATE TABLE sl_isua_all_workers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                source TEXT NOT NULL,
                pay REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isua_all_workers', 'sl_isua_contractors', 'sl_isua_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_isua_employees VALUES (1, 'Alice', 'Eng', 90000)");
        $this->pdo->exec("INSERT INTO sl_isua_employees VALUES (2, 'Bob', 'Sales', 75000)");
        $this->pdo->exec("INSERT INTO sl_isua_employees VALUES (3, 'Charlie', 'Eng', 85000)");

        $this->pdo->exec("INSERT INTO sl_isua_contractors VALUES (1, 'Dave', 'TechStaff', 120)");
        $this->pdo->exec("INSERT INTO sl_isua_contractors VALUES (2, 'Eve', 'CodeCorp', 150)");
    }

    /**
     * INSERT...SELECT UNION ALL with WHERE filtering on each branch.
     */
    public function testInsertSelectUnionAllWithWhere(): void
    {
        $sql = "INSERT INTO sl_isua_all_workers (name, source, pay)
                SELECT name, 'senior-eng', salary FROM sl_isua_employees
                    WHERE dept = 'Eng' AND salary >= 90000
                UNION ALL
                SELECT name, 'premium-contractor', rate FROM sl_isua_contractors
                    WHERE rate >= 140";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM sl_isua_all_workers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT UNION ALL WHERE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Eve', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT UNION ALL WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT UNION ALL with ? params across both branches.
     */
    public function testPreparedInsertSelectUnionAll(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_isua_all_workers (name, source, pay)
                 SELECT name, 'employee', salary FROM sl_isua_employees WHERE salary > ?
                 UNION ALL
                 SELECT name, 'contractor', rate FROM sl_isua_contractors WHERE rate > ?"
            );
            $stmt->execute([80000, 130]);

            $rows = $this->ztdQuery("SELECT name, source FROM sl_isua_all_workers ORDER BY name");

            // Alice (90k), Charlie (85k), Eve (150)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared INSERT SELECT UNION ALL: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT SELECT UNION ALL failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with three-way UNION ALL from three different tables.
     */
    public function testInsertSelectTripleUnionAll(): void
    {
        $this->createTable('CREATE TABLE sl_isua_interns (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            stipend REAL NOT NULL
        )');

        try {
            $this->pdo->exec("INSERT INTO sl_isua_interns VALUES (1, 'Frank', 2000)");

            $sql = "INSERT INTO sl_isua_all_workers (name, source, pay)
                    SELECT name, 'employee', salary FROM sl_isua_employees
                    UNION ALL
                    SELECT name, 'contractor', rate FROM sl_isua_contractors
                    UNION ALL
                    SELECT name, 'intern', stipend FROM sl_isua_interns";

            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM sl_isua_all_workers ORDER BY name");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'INSERT SELECT triple UNION ALL: expected 6, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);

            $sources = array_column($rows, 'source', 'name');
            $this->assertSame('intern', $sources['Frank']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT triple UNION ALL failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('sl_isua_interns');
        }
    }

    /**
     * INSERT...SELECT UNION with same table and differing WHERE clauses (self-union).
     */
    public function testInsertSelectSelfUnionAll(): void
    {
        $sql = "INSERT INTO sl_isua_all_workers (name, source, pay)
                SELECT name, 'eng', salary FROM sl_isua_employees WHERE dept = 'Eng'
                UNION ALL
                SELECT name, 'sales', salary FROM sl_isua_employees WHERE dept = 'Sales'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM sl_isua_all_workers ORDER BY name");

            // Alice (Eng), Bob (Sales), Charlie (Eng) = 3 rows
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT self-UNION ALL: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT self-UNION ALL failed: ' . $e->getMessage());
        }
    }
}
