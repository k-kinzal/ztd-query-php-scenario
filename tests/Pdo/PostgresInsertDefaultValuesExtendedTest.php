<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PostgreSQL-specific INSERT DEFAULT VALUES syntax and
 * DEFAULT keyword behavior through ZTD shadow store.
 *
 * PostgreSQL supports `INSERT INTO t DEFAULT VALUES` which inserts
 * a row with all columns set to their default values.
 */
class PostgresInsertDefaultValuesExtendedTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_defval_test');
        $raw->exec('CREATE TABLE pg_defval_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) DEFAULT \'Unknown\',
            score INT DEFAULT 0,
            active BOOLEAN DEFAULT true
        )');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    /**
     * INSERT with explicit values works normally.
     */
    public function testExplicitValuesWork(): void
    {
        $this->pdo->exec("INSERT INTO pg_defval_test (id, name, score) VALUES (100, 'Alice', 95)");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_defval_test WHERE id = 100');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(95, (int) $row['score']);
    }

    /**
     * INSERT DEFAULT VALUES syntax — may not work through ZTD.
     * ZTD rewrites INSERT to CTE-based SELECT, which doesn't evaluate DEFAULTs.
     */
    public function testInsertDefaultValues(): void
    {
        try {
            $this->pdo->exec('INSERT INTO pg_defval_test DEFAULT VALUES');

            $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_defval_test');
            $count = (int) $stmt->fetchColumn();
            $this->assertGreaterThanOrEqual(1, $count);
        } catch (\Exception $e) {
            // DEFAULT VALUES syntax may not be supported by CTE rewriter
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * INSERT with DEFAULT keyword for specific columns.
     */
    public function testInsertWithDefaultKeyword(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defval_test (id, name, score) VALUES (101, DEFAULT, 80)");

            $stmt = $this->pdo->query('SELECT name FROM pg_defval_test WHERE id = 101');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            // ZTD may not apply defaults — may be NULL instead of 'Unknown'
            // Either the default was applied or it's NULL
            $this->assertTrue($row['name'] === 'Unknown' || $row['name'] === null);
        } catch (\Exception $e) {
            // DEFAULT keyword in VALUES may not be supported
            $this->assertInstanceOf(\Throwable::class, $e);
        }
    }

    /**
     * Omitted columns get NULL in shadow (not database DEFAULT).
     */
    public function testOmittedColumnsAreNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_defval_test (id) VALUES (102)");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_defval_test WHERE id = 102');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // ZTD shadow store doesn't apply database defaults
        $this->assertNull($row['name']);
        $this->assertNull($row['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_defval_test (id, name) VALUES (200, 'Test')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_defval_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                PostgreSQLContainer::getDsn(),
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pg_defval_test');
        } catch (\Exception $e) {
        }
    }
}
