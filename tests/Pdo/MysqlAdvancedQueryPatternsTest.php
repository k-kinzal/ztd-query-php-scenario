<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests advanced SQL query patterns in ZTD mode on MySQL via PDO:
 * CASE, LIKE, IN, BETWEEN, EXISTS, COALESCE, window functions.
 */
class MysqlAdvancedQueryPatternsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_employees');
        $raw->exec('CREATE TABLE mysql_pdo_adv_employees (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(255), score INT)');

        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_departments');
        $raw->exec('CREATE TABLE mysql_pdo_adv_departments (id INT PRIMARY KEY, emp_id INT, dept_name VARCHAR(255))');

        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_bonus');
        $raw->exec('CREATE TABLE mysql_pdo_adv_bonus (id INT PRIMARY KEY, emp_id INT, amount INT)');

        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_profiles');
        $raw->exec('CREATE TABLE mysql_pdo_adv_profiles (id INT PRIMARY KEY, employee_id INT, nickname VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec('DELETE FROM mysql_pdo_adv_employees');
        $this->pdo->exec('DELETE FROM mysql_pdo_adv_departments');
        $this->pdo->exec('DELETE FROM mysql_pdo_adv_bonus');
        $this->pdo->exec('DELETE FROM mysql_pdo_adv_profiles');

        $this->pdo->exec("INSERT INTO mysql_pdo_adv_employees (id, name, department, score) VALUES (1, 'Alice', 'Engineering', 90)");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_employees (id, name, department, score) VALUES (2, 'Bob', 'Sales', 60)");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_employees (id, name, department, score) VALUES (3, 'Charlie', 'Engineering', 110)");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_employees (id, name, department, score) VALUES (4, 'Diana', 'Marketing', 75)");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_employees (id, name, department, score) VALUES (5, 'Eve', 'Sales', 55)");
    }

    public function testCaseExpression(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                CASE
                    WHEN score >= 100 THEN 'high'
                    WHEN score >= 70 THEN 'medium'
                    ELSE 'low'
                END AS score_band
            FROM mysql_pdo_adv_employees ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('medium', $rows[0]['score_band']); // Alice 90
        $this->assertSame('low', $rows[1]['score_band']);     // Bob 60
        $this->assertSame('high', $rows[2]['score_band']);    // Charlie 110
    }

    public function testLikeClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_pdo_adv_employees WHERE name LIKE 'A%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testLikeClauseWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_pdo_adv_employees WHERE name LIKE '%li%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows); // Alice, Charlie
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testInClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_pdo_adv_employees WHERE department IN ('Engineering', 'Marketing') ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);
    }

    public function testBetweenClause(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_pdo_adv_employees WHERE score BETWEEN 60 AND 80 ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    public function testExistsSubquery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_departments (id, emp_id, dept_name) VALUES (1, 1, 'Engineering')");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_departments (id, emp_id, dept_name) VALUES (2, 3, 'Engineering')");

        $stmt = $this->pdo->query("
            SELECT name FROM mysql_pdo_adv_employees e
            WHERE EXISTS (SELECT 1 FROM mysql_pdo_adv_departments d WHERE d.emp_id = e.id)
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testNotExistsSubquery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_bonus (id, emp_id, amount) VALUES (1, 1, 500)");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_bonus (id, emp_id, amount) VALUES (2, 2, 300)");

        $stmt = $this->pdo->query("
            SELECT name FROM mysql_pdo_adv_employees e
            WHERE NOT EXISTS (SELECT 1 FROM mysql_pdo_adv_bonus b WHERE b.emp_id = e.id)
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    public function testCoalesce(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_profiles (id, employee_id, nickname) VALUES (1, 1, 'Ally')");
        $this->pdo->exec("INSERT INTO mysql_pdo_adv_profiles (id, employee_id, nickname) VALUES (2, 2, NULL)");

        $stmt = $this->pdo->query("
            SELECT e.name, COALESCE(p.nickname, 'no_nickname') AS display_name
            FROM mysql_pdo_adv_employees e
            JOIN mysql_pdo_adv_profiles p ON p.employee_id = e.id
            ORDER BY e.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Ally', $rows[0]['display_name']);
        $this->assertSame('no_nickname', $rows[1]['display_name']);
    }

    public function testWindowFunctionRowNumber(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, department, score,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY score DESC) AS rank_in_dept
            FROM mysql_pdo_adv_employees ORDER BY department, rank_in_dept
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(5, $rows);

        // Engineering: Charlie (110) rank 1, Alice (90) rank 2
        $engineering = array_values(array_filter($rows, fn($r) => $r['department'] === 'Engineering'));
        $this->assertSame('Charlie', $engineering[0]['name']);
        $this->assertSame(1, (int) $engineering[0]['rank_in_dept']);
        $this->assertSame('Alice', $engineering[1]['name']);
        $this->assertSame(2, (int) $engineering[1]['rank_in_dept']);
    }

    public function testWindowFunctionSum(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, department, score,
                SUM(score) OVER (PARTITION BY department) AS dept_total
            FROM mysql_pdo_adv_employees WHERE department = 'Engineering' ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        // Both should show the same department total: 90 + 110 = 200
        $this->assertSame(200, (int) $rows[0]['dept_total']);
        $this->assertSame(200, (int) $rows[1]['dept_total']);
    }

    public function testUpdateWithCaseExpression(): void
    {
        $this->pdo->exec("
            UPDATE mysql_pdo_adv_employees SET score = CASE
                WHEN department = 'Engineering' THEN score + 10
                ELSE score + 5
            END
        ");

        $stmt = $this->pdo->query("SELECT name, score FROM mysql_pdo_adv_employees ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(100, (int) $rows[0]['score']); // Alice: 90 + 10
        $this->assertSame(65, (int) $rows[1]['score']);   // Bob: 60 + 5
        $this->assertSame(120, (int) $rows[2]['score']);  // Charlie: 110 + 10
        $this->assertSame(80, (int) $rows[3]['score']);   // Diana: 75 + 5
        $this->assertSame(60, (int) $rows[4]['score']);   // Eve: 55 + 5
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_profiles');
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_bonus');
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_departments');
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_adv_employees');
    }
}
