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
 * Tests PostgreSQL native ENUM type through ZTD.
 *
 * PostgreSQL uses CREATE TYPE ... AS ENUM for enums, unlike MySQL's
 * column-level ENUM definition. Cross-platform parity with MysqlEnumTypeTest.
 * @spec SPEC-10.2.19
 */
class PostgresEnumTypeTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_enum_tasks');
        $raw->exec('DROP TYPE IF EXISTS pg_task_status');
        $raw->exec('DROP TYPE IF EXISTS pg_task_priority');

        $raw->exec("CREATE TYPE pg_task_status AS ENUM ('open', 'in_progress', 'done', 'cancelled')");
        $raw->exec("CREATE TYPE pg_task_priority AS ENUM ('low', 'medium', 'high', 'critical')");
        $raw->exec('CREATE TABLE pg_enum_tasks (id INT PRIMARY KEY, title VARCHAR(100), status pg_task_status, priority pg_task_priority)');

        $this->pdo = ZtdPdo::fromPdo(new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        ));
    }

    /**
     * INSERT with ENUM values.
     */
    public function testInsertEnumValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Fix login bug', 'open', 'high')");

        $stmt = $this->pdo->query('SELECT status, priority FROM pg_enum_tasks WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('open', $row['status']);
        $this->assertSame('high', $row['priority']);
    }

    /**
     * UPDATE ENUM column.
     */
    public function testUpdateEnumColumn(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Fix login bug', 'open', 'high')");
        $this->pdo->exec("UPDATE pg_enum_tasks SET status = 'in_progress' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT status FROM pg_enum_tasks WHERE id = 1');
        $this->assertSame('in_progress', $stmt->fetchColumn());
    }

    /**
     * WHERE comparison with ENUM.
     */
    public function testWhereWithEnumComparison(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Task A', 'open', 'high')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (2, 'Task B', 'done', 'low')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (3, 'Task C', 'open', 'medium')");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pg_enum_tasks WHERE status = 'open'");
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared statement with ENUM parameter.
     */
    public function testPreparedStatementWithEnum(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Task A', 'open', 'high')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (2, 'Task B', 'in_progress', 'medium')");

        $stmt = $this->pdo->prepare('SELECT title FROM pg_enum_tasks WHERE status = ?');
        $stmt->execute(['open']);
        $titles = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Task A'], $titles);
    }

    /**
     * NULL ENUM value.
     */
    public function testNullEnumValue(): void
    {
        $this->pdo->exec('INSERT INTO pg_enum_tasks VALUES (1, \'Unclassified\', NULL, NULL)');

        $stmt = $this->pdo->query('SELECT status, priority FROM pg_enum_tasks WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['status']);
        $this->assertNull($row['priority']);
    }

    /**
     * Multiple ENUM values with filtering and ordering.
     */
    public function testEnumFilterAndOrder(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Task A', 'done', 'low')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (2, 'Task B', 'open', 'critical')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (3, 'Task C', 'open', 'high')");
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (4, 'Task D', 'in_progress', 'medium')");

        $stmt = $this->pdo->query("SELECT title FROM pg_enum_tasks WHERE status != 'done' ORDER BY title");
        $titles = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Task B', 'Task C', 'Task D'], $titles);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_enum_tasks VALUES (1, 'Task A', 'open', 'high')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_enum_tasks');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pg_enum_tasks');
            $raw->exec('DROP TYPE IF EXISTS pg_task_status');
            $raw->exec('DROP TYPE IF EXISTS pg_task_priority');
        } catch (\Exception $e) {
        }
    }
}
