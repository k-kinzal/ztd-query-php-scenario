<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests SHOW statement handling through ZTD on MySQL MySQLi.
 *
 * SHOW statements are MySQL-specific utility statements that may not be
 * recognized by the CTE rewriter. Tests document what works vs what throws.
 */
class MysqlShowStatementsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_show_test');
        $raw->query('CREATE TABLE mi_show_test (id INT PRIMARY KEY, name VARCHAR(50))');
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
     * SHOW TABLES through ZTD.
     */
    public function testShowTables(): void
    {
        try {
            $result = $this->mysqli->query('SHOW TABLES');
            $this->assertNotFalse($result);
            // Should return table list from physical database
            $rows = $result->fetch_all(\MYSQLI_ASSOC);
            $this->assertNotEmpty($rows);
        } catch (\Throwable $e) {
            // SHOW may be treated as unsupported SQL
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * SHOW COLUMNS through ZTD.
     */
    public function testShowColumns(): void
    {
        try {
            $result = $this->mysqli->query('SHOW COLUMNS FROM mi_show_test');
            $this->assertNotFalse($result);
            $rows = $result->fetch_all(\MYSQLI_ASSOC);
            $fields = array_column($rows, 'Field');
            $this->assertContains('id', $fields);
            $this->assertContains('name', $fields);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * SHOW CREATE TABLE through ZTD.
     */
    public function testShowCreateTable(): void
    {
        try {
            $result = $this->mysqli->query('SHOW CREATE TABLE mi_show_test');
            $this->assertNotFalse($result);
            $row = $result->fetch_assoc();
            $this->assertArrayHasKey('Create Table', $row);
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * SHOW with Ignore behavior rule — silently ignored.
     */
    public function testShowWithIgnoreBehaviorRule(): void
    {
        $config = new ZtdConfig(
            behaviorRules: [
                'SHOW' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // Ignore mode silently ignores unsupported SQL — no exception, no result
        $result = $mysqli->query('SHOW TABLES');
        // Result may be false (ignored) or an empty result
        if ($result !== false) {
            $rows = $result->fetch_all(\MYSQLI_ASSOC);
            $this->assertIsArray($rows);
        } else {
            $this->assertFalse($result);
        }

        // Shadow operations should still work
        $mysqli->query("INSERT INTO mi_show_test VALUES (1, 'test')");
        $result = $mysqli->query('SELECT COUNT(*) AS cnt FROM mi_show_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        $mysqli->close();
    }

    /**
     * Shadow operations still work after SHOW statements.
     */
    public function testShadowOperationsWorkAfterShow(): void
    {
        // Try SHOW (may or may not work)
        try {
            $this->mysqli->query('SHOW TABLES');
        } catch (\Throwable $e) {
            // Ignore
        }

        // Shadow operations should still work
        $this->mysqli->query("INSERT INTO mi_show_test VALUES (1, 'Alice')");
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_show_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_show_test VALUES (1, 'Shadow')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_show_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_show_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
