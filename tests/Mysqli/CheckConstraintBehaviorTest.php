<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests CHECK constraint behavior with ZTD shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlCheckConstraintBehaviorTest (PDO).
 * CHECK constraints are NOT enforced in shadow.
 */
class CheckConstraintBehaviorTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_check_test');
        $raw->query("CREATE TABLE mi_check_test (
            id INT PRIMARY KEY,
            age INT,
            score INT,
            status VARCHAR(20),
            CONSTRAINT mi_chk_age CHECK (age >= 0 AND age <= 150),
            CONSTRAINT mi_chk_score CHECK (score >= 0),
            CONSTRAINT mi_chk_status CHECK (status IN ('active', 'inactive', 'pending'))
        )");
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
     * INSERT with valid values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, 25, 100, 'active')");

        $result = $this->mysqli->query('SELECT age, status FROM mi_check_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK succeeds in shadow.
     */
    public function testInsertViolatingCheckSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, -1, 100, 'active')");

        $result = $this->mysqli->query('SELECT age FROM mi_check_test WHERE id = 1');
        $this->assertSame(-1, (int) $result->fetch_assoc()['age']);
    }

    /**
     * UPDATE violating CHECK succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, 25, 100, 'active')");
        $this->mysqli->query('UPDATE mi_check_test SET age = 200 WHERE id = 1');

        $result = $this->mysqli->query('SELECT age FROM mi_check_test WHERE id = 1');
        $this->assertSame(200, (int) $result->fetch_assoc()['age']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_check_test VALUES (1, -1, -999, 'bad')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_check_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_check_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
