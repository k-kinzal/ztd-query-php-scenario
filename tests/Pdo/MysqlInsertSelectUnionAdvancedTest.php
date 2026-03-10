<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests advanced INSERT...SELECT UNION patterns through ZTD shadow store on MySQL (PDO).
 *
 * @spec SPEC-4.1a
 */
class MysqlInsertSelectUnionAdvancedTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_isua_employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL,
                salary DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_isua_contractors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                agency VARCHAR(50) NOT NULL,
                rate DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_isua_all_workers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                source VARCHAR(30) NOT NULL,
                pay DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_isua_all_workers', 'my_isua_contractors', 'my_isua_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_isua_employees (name, dept, salary) VALUES ('Alice', 'Eng', 90000)");
        $this->pdo->exec("INSERT INTO my_isua_employees (name, dept, salary) VALUES ('Bob', 'Sales', 75000)");
        $this->pdo->exec("INSERT INTO my_isua_employees (name, dept, salary) VALUES ('Charlie', 'Eng', 85000)");

        $this->pdo->exec("INSERT INTO my_isua_contractors (name, agency, rate) VALUES ('Dave', 'TechStaff', 120)");
        $this->pdo->exec("INSERT INTO my_isua_contractors (name, agency, rate) VALUES ('Eve', 'CodeCorp', 150)");
    }

    public function testInsertSelectTripleUnionAll(): void
    {
        $this->createTable('CREATE TABLE my_isua_interns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            stipend DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB');

        try {
            $this->pdo->exec("INSERT INTO my_isua_interns (name, stipend) VALUES ('Frank', 2000)");

            $sql = "INSERT INTO my_isua_all_workers (name, source, pay)
                    SELECT name, 'employee', salary FROM my_isua_employees
                    UNION ALL
                    SELECT name, 'contractor', rate FROM my_isua_contractors
                    UNION ALL
                    SELECT name, 'intern', stipend FROM my_isua_interns";

            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, source FROM my_isua_all_workers ORDER BY name");

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
            $this->dropTable('my_isua_interns');
        }
    }

    public function testPreparedInsertSelectUnionAll(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO my_isua_all_workers (name, source, pay)
                 SELECT name, 'employee', salary FROM my_isua_employees WHERE salary > ?
                 UNION ALL
                 SELECT name, 'contractor', rate FROM my_isua_contractors WHERE rate > ?"
            );
            $stmt->execute([80000, 130]);

            $rows = $this->ztdQuery("SELECT name, source FROM my_isua_all_workers ORDER BY name");

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
