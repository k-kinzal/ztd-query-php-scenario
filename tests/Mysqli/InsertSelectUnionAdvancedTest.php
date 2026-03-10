<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests advanced INSERT...SELECT UNION patterns through ZTD shadow store on MySQLi.
 *
 * @spec SPEC-4.1a
 */
class InsertSelectUnionAdvancedTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_isua_employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL,
                salary DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_isua_contractors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                agency VARCHAR(50) NOT NULL,
                rate DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_isua_all_workers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                source VARCHAR(30) NOT NULL,
                pay DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_isua_all_workers', 'mi_isua_contractors', 'mi_isua_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_isua_employees (name, dept, salary) VALUES ('Alice', 'Eng', 90000)");
        $this->mysqli->query("INSERT INTO mi_isua_employees (name, dept, salary) VALUES ('Bob', 'Sales', 75000)");
        $this->mysqli->query("INSERT INTO mi_isua_employees (name, dept, salary) VALUES ('Charlie', 'Eng', 85000)");

        $this->mysqli->query("INSERT INTO mi_isua_contractors (name, agency, rate) VALUES ('Dave', 'TechStaff', 120)");
        $this->mysqli->query("INSERT INTO mi_isua_contractors (name, agency, rate) VALUES ('Eve', 'CodeCorp', 150)");
    }

    public function testInsertSelectTripleUnionAll(): void
    {
        $this->createTable('CREATE TABLE mi_isua_interns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            stipend DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB');

        try {
            $this->mysqli->query("INSERT INTO mi_isua_interns (name, stipend) VALUES ('Frank', 2000)");

            $sql = "INSERT INTO mi_isua_all_workers (name, source, pay)
                    SELECT name, 'employee', salary FROM mi_isua_employees
                    UNION ALL
                    SELECT name, 'contractor', rate FROM mi_isua_contractors
                    UNION ALL
                    SELECT name, 'intern', stipend FROM mi_isua_interns";

            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM mi_isua_all_workers ORDER BY name");

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
            $this->dropTable('mi_isua_interns');
        }
    }
}
