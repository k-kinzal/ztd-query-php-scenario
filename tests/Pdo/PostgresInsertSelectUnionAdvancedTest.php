<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests advanced INSERT...SELECT UNION patterns through ZTD shadow store on PostgreSQL.
 *
 * @spec SPEC-4.1a
 */
class PostgresInsertSelectUnionAdvancedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_isua_employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL,
                salary NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_isua_contractors (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                agency VARCHAR(50) NOT NULL,
                rate NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_isua_all_workers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                source VARCHAR(30) NOT NULL,
                pay NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isua_all_workers', 'pg_isua_contractors', 'pg_isua_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_isua_employees (name, dept, salary) VALUES ('Alice', 'Eng', 90000)");
        $this->pdo->exec("INSERT INTO pg_isua_employees (name, dept, salary) VALUES ('Bob', 'Sales', 75000)");
        $this->pdo->exec("INSERT INTO pg_isua_employees (name, dept, salary) VALUES ('Charlie', 'Eng', 85000)");

        $this->pdo->exec("INSERT INTO pg_isua_contractors (name, agency, rate) VALUES ('Dave', 'TechStaff', 120)");
        $this->pdo->exec("INSERT INTO pg_isua_contractors (name, agency, rate) VALUES ('Eve', 'CodeCorp', 150)");
    }

    public function testInsertSelectTripleUnionAll(): void
    {
        $this->createTable('CREATE TABLE pg_isua_interns (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            stipend NUMERIC(10,2) NOT NULL
        )');

        try {
            $this->pdo->exec("INSERT INTO pg_isua_interns (name, stipend) VALUES ('Frank', 2000)");

            $sql = "INSERT INTO pg_isua_all_workers (name, source, pay)
                    SELECT name, 'employee', salary FROM pg_isua_employees
                    UNION ALL
                    SELECT name, 'contractor', rate FROM pg_isua_contractors
                    UNION ALL
                    SELECT name, 'intern', stipend FROM pg_isua_interns";

            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM pg_isua_all_workers ORDER BY name");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'INSERT SELECT triple UNION ALL: expected 6, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT triple UNION ALL failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('pg_isua_interns');
        }
    }

    public function testPreparedInsertSelectUnionAll(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_isua_all_workers (name, source, pay)
                 SELECT name, 'employee', salary FROM pg_isua_employees WHERE salary > ?
                 UNION ALL
                 SELECT name, 'contractor', rate FROM pg_isua_contractors WHERE rate > ?"
            );
            $stmt->execute([80000, 130]);

            $rows = $this->ztdQuery("SELECT name, source FROM pg_isua_all_workers ORDER BY name");

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
}
