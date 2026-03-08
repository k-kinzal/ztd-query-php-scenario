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
 * Tests CHECK constraint behavior with ZTD shadow store on PostgreSQL.
 *
 * PostgreSQL has full CHECK constraint support. Since ZTD rewrites
 * operations to CTE-based queries, CHECK constraints are NOT enforced
 * in shadow.
 */
class PostgresCheckConstraintBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_check_test');
        $raw->exec("CREATE TABLE pg_check_test (
            id INT PRIMARY KEY,
            age INT CHECK (age >= 0 AND age <= 150),
            score INT CHECK (score >= 0),
            status VARCHAR(20) CHECK (status IN ('active', 'inactive', 'pending'))
        )");
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');
    }

    /**
     * INSERT with valid values succeeds.
     */
    public function testInsertWithValidValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age, status FROM pg_check_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['age']);
        $this->assertSame('active', $row['status']);
    }

    /**
     * INSERT violating CHECK succeeds in shadow.
     */
    public function testInsertViolatingCheckSucceedsInShadow(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, -1, 100, 'active')");

        $stmt = $this->pdo->query('SELECT age FROM pg_check_test WHERE id = 1');
        $this->assertSame(-1, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT with invalid status succeeds in shadow.
     */
    public function testInsertInvalidStatusSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'invalid')");

        $stmt = $this->pdo->query('SELECT status FROM pg_check_test WHERE id = 1');
        $this->assertSame('invalid', $stmt->fetchColumn());
    }

    /**
     * UPDATE violating CHECK succeeds in shadow.
     */
    public function testUpdateViolatingCheckSucceeds(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, 25, 100, 'active')");
        $this->pdo->exec('UPDATE pg_check_test SET age = 200 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT age FROM pg_check_test WHERE id = 1');
        $this->assertSame(200, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_check_test VALUES (1, -1, -999, 'bad')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_check_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_check_test');
        } catch (\Exception $e) {
        }
    }
}
