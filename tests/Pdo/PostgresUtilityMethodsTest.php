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
 * Tests utility methods: getAvailableDrivers, lastInsertId, errorCode, errorInfo,
 * setAttribute/getAttribute on PostgreSQL via PDO.
 */
class PostgresUtilityMethodsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_util_test');
        $raw->exec('CREATE TABLE pg_util_test (id SERIAL PRIMARY KEY, val VARCHAR(255))');
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

    public function testGetAvailableDrivers(): void
    {
        $drivers = ZtdPdo::getAvailableDrivers();
        $this->assertIsArray($drivers);
        $this->assertContains('pgsql', $drivers);
    }

    public function testLastInsertIdAfterShadowInsertThrows(): void
    {
        $this->pdo->exec("INSERT INTO pg_util_test (val) VALUES ('first')");

        // PostgreSQL lastInsertId requires sequence name and throws because
        // shadow inserts don't reach the physical DB, so the sequence is
        // never advanced ("currval of sequence is not yet defined in this session")
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/not yet defined/');
        $this->pdo->lastInsertId('pg_util_test_id_seq');
    }

    public function testErrorCodeAndErrorInfo(): void
    {
        $this->pdo->exec("INSERT INTO pg_util_test (val) VALUES ('test')");
        $this->assertSame('00000', $this->pdo->errorCode());

        $errorInfo = $this->pdo->errorInfo();
        $this->assertIsArray($errorInfo);
        $this->assertSame('00000', $errorInfo[0]);
    }

    public function testSetAndGetAttribute(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $mode = $this->pdo->getAttribute(PDO::ATTR_DEFAULT_FETCH_MODE);
        $this->assertSame(PDO::FETCH_ASSOC, $mode);
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString("it", $quoted);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_util_test');
    }
}
