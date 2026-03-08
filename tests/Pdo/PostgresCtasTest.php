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
 * Tests CREATE TABLE AS SELECT (CTAS) on PostgreSQL PDO.
 *
 * CTAS creates a new shadow table and populates it. However:
 * - Column types default to TEXT (no schema reflection), so type comparisons
 *   may require explicit casting.
 * - CTAS with empty result set (WHERE 1=0) throws "Cannot determine columns"
 *   because column inference requires at least one row.
 */
class PostgresCtasTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_ctas_copy');
        $raw->exec('DROP TABLE IF EXISTS pg_ctas_filtered');
        $raw->exec('DROP TABLE IF EXISTS pg_ctas_source');
        $raw->exec('CREATE TABLE pg_ctas_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (1, 'Alice', 95)");
        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pg_ctas_source VALUES (3, 'Charlie', 75)");
    }

    /**
     * CTAS copies shadow data. Column types default to TEXT,
     * requiring string comparison (not integer).
     */
    public function testCtasCopiesData(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctas_copy AS SELECT * FROM pg_ctas_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_copy');
        $this->assertEquals(3, (int) $stmt->fetchColumn());

        // Column types are TEXT in shadow, so use string comparison
        $stmt = $this->pdo->query("SELECT name FROM pg_ctas_copy WHERE id = '1'");
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CTAS with WHERE filter.
     */
    public function testCtasWithFilter(): void
    {
        $this->pdo->exec("CREATE TABLE pg_ctas_filtered AS SELECT * FROM pg_ctas_source WHERE score >= 85");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_filtered');
        $this->assertEquals(2, (int) $stmt->fetchColumn());
    }

    /**
     * CTAS with empty result throws — can't determine columns from 0 rows.
     */
    public function testCtasEmptyResultThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot determine columns');
        $this->pdo->exec('CREATE TABLE pg_ctas_empty AS SELECT * FROM pg_ctas_source WHERE 1=0');
    }

    /**
     * CTAS column types default to TEXT — integer comparison fails.
     */
    public function testCtasColumnTypesDefaultToText(): void
    {
        $this->pdo->exec('CREATE TABLE pg_ctas_copy AS SELECT * FROM pg_ctas_source');

        // Integer comparison fails because column is TEXT
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT name FROM pg_ctas_copy WHERE id = 1');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctas_source');
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
            $raw->exec('DROP TABLE IF EXISTS pg_ctas_copy');
            $raw->exec('DROP TABLE IF EXISTS pg_ctas_filtered');
            $raw->exec('DROP TABLE IF EXISTS pg_ctas_source');
        } catch (\Exception $e) {
        }
    }
}
