<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests error handling across ZTD toggle boundaries on MySQL:
 * - Errors when ZTD is enabled vs disabled
 * - State consistency after toggle + error
 * - Prepared statements created with ZTD on/off and executed across toggles
 * - Shadow data visibility vs physical data across toggles
 */
class MysqlZtdToggleErrorHandlingTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mzte_users');
        $raw->exec('CREATE TABLE mzte_users (id INT PRIMARY KEY, name VARCHAR(50))');
        // Insert physical data so it exists when ZTD is disabled
        $raw->exec("INSERT INTO mzte_users VALUES (1, 'Alice')");
        $raw->exec("INSERT INTO mzte_users VALUES (2, 'Bob')");
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        // Insert through ZtdPdo so shadow store has the data
        // This shadows the physical data with the same values
        $this->pdo->exec("INSERT INTO mzte_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO mzte_users VALUES (2, 'Bob')");
    }

    public function testErrorWithZtdEnabledDoesNotCorruptShadow(): void
    {
        // Error with ZTD on
        try {
            $this->pdo->query('SELECT * FROM nonexistent_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        // Shadow data still intact
        $stmt = $this->pdo->query('SELECT name FROM mzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testErrorWithZtdDisabledThenReEnable(): void
    {
        // Disable ZTD, error on physical DB
        $this->pdo->disableZtd();

        try {
            $this->pdo->query('SELECT * FROM nonexistent_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        // Re-enable ZTD
        $this->pdo->enableZtd();

        // Shadow data still intact from physical table
        $stmt = $this->pdo->query('SELECT name FROM mzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testToggleAfterErrorPreservesState(): void
    {
        // Insert data in ZTD, then error, then toggle
        $this->pdo->exec("INSERT INTO mzte_users VALUES (3, 'Charlie')");

        try {
            $this->pdo->exec('INSERT INTO nonexistent_xyz VALUES (1)');
        } catch (\Exception $e) {
            // Expected
        }

        // Toggle off and on
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Shadow data should persist across toggle
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mzte_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testPreparedStatementSurvivesToggle(): void
    {
        // Prepare with ZTD on
        $stmt = $this->pdo->prepare('SELECT name FROM mzte_users WHERE id = ?');

        // Toggle ZTD off and on
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Execute — should still use the ZTD-rewritten query
        $stmt->execute([1]);
        $name = $stmt->fetchColumn();
        $this->assertSame('Alice', $name);
    }

    public function testShadowInsertNotVisibleWhenDisabled(): void
    {
        // Insert in ZTD mode (shadow-only)
        $this->pdo->exec("INSERT INTO mzte_users VALUES (4, 'ShadowUser')");

        // Verify shadow sees 3 rows (2 physical + 1 shadow)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mzte_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // Disable ZTD — queries go to physical DB
        $this->pdo->disableZtd();

        // Physical table only has 2 rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mzte_users');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testMultipleToggleCyclesAccumulateShadow(): void
    {
        // Cycle 1: insert in ZTD
        $this->pdo->exec("INSERT INTO mzte_users VALUES (3, 'Charlie')");
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Cycle 2: insert more in ZTD (shadow should persist)
        $this->pdo->exec("INSERT INTO mzte_users VALUES (4, 'Diana')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mzte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());

        // Cycle 3: toggle again, data still there
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mzte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    public function testPhysicalQueryAfterDisableSeesOriginalData(): void
    {
        // Modify data in ZTD mode
        $this->pdo->exec("UPDATE mzte_users SET name = 'Modified' WHERE id = 1");

        // ZTD sees modification
        $stmt = $this->pdo->query('SELECT name FROM mzte_users WHERE id = 1');
        $this->assertSame('Modified', $stmt->fetchColumn());

        // Disable ZTD — physical DB still has original
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT name FROM mzte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mzte_users');
    }
}
