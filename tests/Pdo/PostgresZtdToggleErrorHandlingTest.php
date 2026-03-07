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
 * Tests error handling across ZTD toggle boundaries on PostgreSQL:
 * - Errors when ZTD is enabled vs disabled
 * - State consistency after toggle + error
 * - Prepared statements created with ZTD on/off and executed across toggles
 * - Shadow data visibility vs physical data across toggles
 */
class PostgresZtdToggleErrorHandlingTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pgzte_users');
        $raw->exec('CREATE TABLE pgzte_users (id INT PRIMARY KEY, name VARCHAR(50))');
        // Insert physical data so it exists when ZTD is disabled
        $raw->exec("INSERT INTO pgzte_users VALUES (1, 'Alice')");
        $raw->exec("INSERT INTO pgzte_users VALUES (2, 'Bob')");
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        // Insert through ZtdPdo so shadow store has the data
        // This shadows the physical data with the same values
        $this->pdo->exec("INSERT INTO pgzte_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pgzte_users VALUES (2, 'Bob')");
    }

    public function testErrorWithZtdEnabledDoesNotCorruptShadow(): void
    {
        try {
            $this->pdo->query('SELECT * FROM nonexistent_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT name FROM pgzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testErrorWithZtdDisabledThenReEnable(): void
    {
        $this->pdo->disableZtd();

        try {
            $this->pdo->query('SELECT * FROM nonexistent_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT name FROM pgzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testToggleAfterErrorPreservesState(): void
    {
        $this->pdo->exec("INSERT INTO pgzte_users VALUES (3, 'Charlie')");

        try {
            $this->pdo->exec('INSERT INTO nonexistent_xyz VALUES (1)');
        } catch (\Exception $e) {
            // Expected
        }

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pgzte_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testPreparedStatementSurvivesToggle(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pgzte_users WHERE id = ?');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt->execute([1]);
        $name = $stmt->fetchColumn();
        $this->assertSame('Alice', $name);
    }

    public function testShadowInsertNotVisibleWhenDisabled(): void
    {
        // Insert extra row in ZTD mode (shadow-only)
        $this->pdo->exec("INSERT INTO pgzte_users VALUES (4, 'ShadowUser')");

        // Shadow sees 3 rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pgzte_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // Disable ZTD — physical DB only has original 2 rows
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pgzte_users');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testMultipleToggleCyclesAccumulateShadow(): void
    {
        $this->pdo->exec("INSERT INTO pgzte_users VALUES (3, 'Charlie')");
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $this->pdo->exec("INSERT INTO pgzte_users VALUES (4, 'Diana')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pgzte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pgzte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    public function testPhysicalQueryAfterDisableSeesOriginalData(): void
    {
        // Modify data in ZTD mode (shadow only)
        $this->pdo->exec("UPDATE pgzte_users SET name = 'Modified' WHERE id = 1");

        // ZTD sees modification
        $stmt = $this->pdo->query('SELECT name FROM pgzte_users WHERE id = 1');
        $this->assertSame('Modified', $stmt->fetchColumn());

        // Disable ZTD — physical DB still has original
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT name FROM pgzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pgzte_users');
    }
}
