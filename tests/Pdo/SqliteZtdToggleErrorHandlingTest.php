<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests error handling across ZTD toggle boundaries:
 * - Errors when ZTD is enabled vs disabled
 * - State consistency after toggle + error
 * - Prepared statements created with ZTD on/off and executed across toggles
 * @spec pending
 */
class SqliteZtdToggleErrorHandlingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE zte_users (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['zte_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO zte_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO zte_users VALUES (2, 'Bob')");
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
        $stmt = $this->pdo->query('SELECT name FROM zte_users WHERE id = 1');
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

        // Shadow data still intact from before disable
        $stmt = $this->pdo->query('SELECT name FROM zte_users WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public function testToggleAfterErrorPreservesState(): void
    {
        // Insert data, then error, then toggle
        $this->pdo->exec("INSERT INTO zte_users VALUES (3, 'Charlie')");

        try {
            $this->pdo->exec('INSERT INTO nonexistent_xyz VALUES (1)');
        } catch (\Exception $e) {
            // Expected
        }

        // Toggle off and on
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Shadow data should persist across toggle
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM zte_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testPreparedStatementSurvivesToggle(): void
    {
        // Prepare with ZTD on
        $stmt = $this->pdo->prepare('SELECT name FROM zte_users WHERE id = ?');

        // Toggle ZTD off and on
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Execute — should still use the ZTD-rewritten query (prepared at ZTD-on time)
        $stmt->execute([1]);
        $name = $stmt->fetchColumn();
        $this->assertSame('Alice', $name);
    }

    public function testInsertDuringDisabledGoesToPhysicalTable(): void
    {
        // Table exists physically (created by raw PDO in setUp), so INSERT succeeds
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO zte_users VALUES (3, 'Charlie')");

        // Physical table now has 1 row
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM zte_users');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testMultipleToggleCyclesAccumulateShadow(): void
    {
        // Cycle 1: insert in ZTD
        $this->pdo->exec("INSERT INTO zte_users VALUES (3, 'Charlie')");
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Cycle 2: insert more in ZTD (shadow should persist)
        $this->pdo->exec("INSERT INTO zte_users VALUES (4, 'Diana')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM zte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());

        // Cycle 3: toggle again, data still there
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM zte_users');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    public function testQueryPhysicalDuringDisabledReturnsNoShadowData(): void
    {
        // Insert shadow data
        $this->pdo->exec("INSERT INTO zte_users VALUES (3, 'ShadowUser')");

        // Disable ZTD — physical table exists but has no data (all writes were shadow-only)
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM zte_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
