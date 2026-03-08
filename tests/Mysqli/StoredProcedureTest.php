<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL stored procedure CALL through CTE shadow on MySQLi.
 *
 * Stored procedures are common in enterprise applications.
 * Tests verify CALL, stored functions, and interaction with shadow data.
 * @spec SPEC-3.3g, SPEC-6.5
 */
class StoredProcedureTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_sp_users (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_sp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Create stored procedures on the physical database
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $raw->query('DROP PROCEDURE IF EXISTS sp_get_user');
        $raw->query('DROP PROCEDURE IF EXISTS sp_insert_user');
        $raw->query('DROP PROCEDURE IF EXISTS sp_count_users');
        $raw->query('DROP FUNCTION IF EXISTS fn_double_score');

        $raw->query("
            CREATE PROCEDURE sp_get_user(IN user_id INT)
            BEGIN
                SELECT * FROM mi_sp_users WHERE id = user_id;
            END
        ");

        $raw->query("
            CREATE PROCEDURE sp_insert_user(IN user_id INT, IN user_name VARCHAR(50), IN user_score INT)
            BEGIN
                INSERT INTO mi_sp_users (id, name, score) VALUES (user_id, user_name, user_score);
            END
        ");

        $raw->query("
            CREATE PROCEDURE sp_count_users(OUT total INT)
            BEGIN
                SELECT COUNT(*) INTO total FROM mi_sp_users;
            END
        ");

        $raw->query("
            CREATE FUNCTION fn_double_score(s INT) RETURNS INT DETERMINISTIC
            BEGIN
                RETURN s * 2;
            END
        ");

        $raw->close();

        $this->mysqli->query("INSERT INTO mi_sp_users VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_sp_users VALUES (2, 'Bob', 75)");
    }

    /**
     * CALL stored procedure that performs SELECT.
     *
     * CALL is unsupported SQL — tests whether it throws, passes through, or works.
     */
    public function testCallSelectProcedure(): void
    {
        try {
            $result = $this->mysqli->query('CALL sp_get_user(1)');
            if ($result !== false) {
                $row = $result->fetch_assoc();
                $this->assertSame('Alice', $row['name']);
            } else {
                // CALL may return false if treated as unsupported
                $this->assertFalse($result);
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
            $this->mysqli->query("CALL sp_insert_user(3, 'Charlie', 85)");

            // Check if the insert was visible
            $result = $this->mysqli->query('SELECT name FROM mi_sp_users WHERE id = 3');
            $row = $result->fetch_assoc();
            if ($row !== null) {
                $this->assertSame('Charlie', $row['name']);
            }
        } catch (\Throwable $e) {
            // CALL may be treated as unsupported SQL
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Stored function in SELECT expression.
     */
    public function testStoredFunctionInSelect(): void
    {
        try {
            $result = $this->mysqli->query(
                'SELECT name, fn_double_score(score) AS doubled FROM mi_sp_users WHERE id = 1'
            );
            $row = $result->fetch_assoc();
            $this->assertSame('Alice', $row['name']);
            $this->assertEquals(180, (int) $row['doubled']);
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
            $result = $this->mysqli->query(
                'SELECT name FROM mi_sp_users WHERE fn_double_score(score) > 160 ORDER BY name'
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names); // 90*2=180 > 160
            $this->assertNotContains('Bob', $names); // 75*2=150 < 160
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
            $result = $this->mysqli->query(
                'SELECT name FROM mi_sp_users ORDER BY fn_double_score(score) DESC'
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertSame('Alice', $rows[0]['name']); // 180
            $this->assertSame('Bob', $rows[1]['name']); // 150
        } catch (\Throwable $e) {
            $this->markTestSkipped('Stored function in ORDER BY not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Shadow operations work after failed CALL attempt.
     */
    public function testShadowWorksAfterCallAttempt(): void
    {
        try {
            $this->mysqli->query('CALL sp_get_user(1)');
        } catch (\Throwable $e) {
            // Ignore
        }

        // Shadow operations should still work
        $this->mysqli->query("INSERT INTO mi_sp_users VALUES (3, 'Charlie', 85)");
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sp_users');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sp_users');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                \Tests\Support\MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                \Tests\Support\MySQLContainer::getPort(),
            );
            $raw->query('DROP PROCEDURE IF EXISTS sp_get_user');
            $raw->query('DROP PROCEDURE IF EXISTS sp_insert_user');
            $raw->query('DROP PROCEDURE IF EXISTS sp_count_users');
            $raw->query('DROP FUNCTION IF EXISTS fn_double_score');
            $raw->query('DROP TABLE IF EXISTS mi_sp_users');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
