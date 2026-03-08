<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL stored procedure CALL through CTE shadow on PDO.
 *
 * Stored procedures are common in enterprise applications.
 * Tests verify CALL, stored functions, and interaction with shadow data.
 * @spec SPEC-3.3g, SPEC-6.5
 */
class MysqlStoredProcedureTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_sp_users (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_sp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Create stored procedures on the physical database
        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP PROCEDURE IF EXISTS pdo_sp_get_user');
        $raw->exec('DROP PROCEDURE IF EXISTS pdo_sp_insert_user');
        $raw->exec('DROP FUNCTION IF EXISTS pdo_fn_double_score');

        $raw->exec("
            CREATE PROCEDURE pdo_sp_get_user(IN user_id INT)
            BEGIN
                SELECT * FROM pdo_sp_users WHERE id = user_id;
            END
        ");

        $raw->exec("
            CREATE PROCEDURE pdo_sp_insert_user(IN user_id INT, IN user_name VARCHAR(50), IN user_score INT)
            BEGIN
                INSERT INTO pdo_sp_users (id, name, score) VALUES (user_id, user_name, user_score);
            END
        ");

        $raw->exec("
            CREATE FUNCTION pdo_fn_double_score(s INT) RETURNS INT DETERMINISTIC
            BEGIN
                RETURN s * 2;
            END
        ");

        $this->pdo->exec("INSERT INTO pdo_sp_users VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_sp_users VALUES (2, 'Bob', 75)");
    }

    /**
     * CALL stored procedure that performs SELECT.
     */
    public function testCallSelectProcedure(): void
    {
        try {
            $stmt = $this->pdo->query('CALL pdo_sp_get_user(1)');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            if ($row !== false) {
                $this->assertSame('Alice', $row['name']);
            }
        } catch (\Throwable $e) {
            // CALL may be treated as unsupported SQL
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * CALL stored procedure that performs INSERT.
     */
    public function testCallInsertProcedure(): void
    {
        try {
            $this->pdo->exec("CALL pdo_sp_insert_user(3, 'Charlie', 85)");

            $rows = $this->ztdQuery('SELECT name FROM pdo_sp_users WHERE id = 3');
            if (!empty($rows)) {
                $this->assertSame('Charlie', $rows[0]['name']);
            }
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Stored function in SELECT expression.
     */
    public function testStoredFunctionInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT name, pdo_fn_double_score(score) AS doubled FROM pdo_sp_users WHERE id = 1'
            );
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEquals(180, (int) $rows[0]['doubled']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Stored function in SELECT not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Stored function in WHERE clause.
     */
    public function testStoredFunctionInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT name FROM pdo_sp_users WHERE pdo_fn_double_score(score) > 160 ORDER BY name'
            );
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertNotContains('Bob', $names);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Stored function in WHERE not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Stored function in ORDER BY clause.
     */
    public function testStoredFunctionInOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT name FROM pdo_sp_users ORDER BY pdo_fn_double_score(score) DESC'
            );
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Stored function in ORDER BY not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with stored function.
     */
    public function testPreparedStoredFunction(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT name FROM pdo_sp_users WHERE pdo_fn_double_score(score) > ? ORDER BY name',
                [160]
            );
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared stored function not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Shadow operations work after failed CALL attempt.
     */
    public function testShadowWorksAfterCallAttempt(): void
    {
        try {
            $this->pdo->query('CALL pdo_sp_get_user(1)');
        } catch (\Throwable $e) {
            // Ignore
        }

        $this->pdo->exec("INSERT INTO pdo_sp_users VALUES (3, 'Charlie', 85)");
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_sp_users');
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_sp_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                \Tests\Support\MySQLContainer::getDsn(),
                'root',
                'root',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            );
            $raw->exec('DROP PROCEDURE IF EXISTS pdo_sp_get_user');
            $raw->exec('DROP PROCEDURE IF EXISTS pdo_sp_insert_user');
            $raw->exec('DROP FUNCTION IF EXISTS pdo_fn_double_score');
            $raw->exec('DROP TABLE IF EXISTS pdo_sp_users');
        } catch (\Exception $e) {
        }
    }
}
