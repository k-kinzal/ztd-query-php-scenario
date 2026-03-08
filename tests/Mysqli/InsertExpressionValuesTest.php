<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests INSERT with SQL expressions in VALUES clause via MySQLi.
 *
 * Cross-platform parity with MysqlInsertExpressionValuesTest (PDO).
 */
class InsertExpressionValuesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_expr_test');
        $raw->query('CREATE TABLE mi_expr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, label VARCHAR(50))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    /**
     * INSERT with arithmetic expression.
     */
    public function testInsertWithArithmeticExpression(): void
    {
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, 'Alice', 40 + 50, 'computed')");

        $result = $this->mysqli->query('SELECT score FROM mi_expr_test WHERE id = 1');
        $this->assertSame(90, (int) $result->fetch_assoc()['score']);
    }

    /**
     * INSERT with string function.
     */
    public function testInsertWithStringFunction(): void
    {
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, UPPER('alice'), 100, CONCAT('test', '_label'))");

        $result = $this->mysqli->query('SELECT name, label FROM mi_expr_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('ALICE', $row['name']);
        $this->assertSame('test_label', $row['label']);
    }

    /**
     * INSERT with COALESCE.
     */
    public function testInsertWithCoalesce(): void
    {
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, COALESCE(NULL, 'Fallback'), 0, 'ok')");

        $result = $this->mysqli->query('SELECT name FROM mi_expr_test WHERE id = 1');
        $this->assertSame('Fallback', $result->fetch_assoc()['name']);
    }

    /**
     * INSERT with ABS and negative number.
     */
    public function testInsertWithAbsNegative(): void
    {
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, 'Test', ABS(-42), 'abs')");

        $result = $this->mysqli->query('SELECT score FROM mi_expr_test WHERE id = 1');
        $this->assertSame(42, (int) $result->fetch_assoc()['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_expr_test VALUES (1, 'Test', 100, 'x')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_expr_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_expr_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
