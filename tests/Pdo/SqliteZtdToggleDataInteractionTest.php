<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZTD enable/disable toggle with data interaction patterns.
 *
 * Common real-world patterns where users toggle ZTD:
 * - Physical INSERT → enable ZTD → shadow queries see physical data
 * - Shadow INSERT → disable ZTD → verify physical table empty
 * - Multiple toggle cycles with different data in each state
 * - ZTD disabled physical INSERT, then ZTD enabled shadow INSERT on same table
 * @spec pending
 */
class SqliteZtdToggleDataInteractionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE toggle_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['toggle_test'];
    }


    /**
     * Shadow data persists across disable/enable cycle.
     */
    public function testShadowDataPersistsAcrossToggle(): void
    {
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Shadow', 100)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT name FROM toggle_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow', $row['name']);
    }

    /**
     * Physical INSERT while ZTD disabled, then re-enable ZTD.
     *
     * After re-enabling ZTD, the physical data should NOT be visible
     * because ZTD shadow store replaces the physical table entirely.
     */
    public function testPhysicalInsertThenReenableZtd(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Physical', 50)");
        $this->pdo->enableZtd();

        // ZTD shadow store starts empty — physical data is hidden
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM toggle_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical INSERT while disabled, then shadow INSERT while enabled.
     *
     * Only shadow data should be visible when ZTD is enabled.
     */
    public function testPhysicalThenShadowInsert(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Physical', 50)");
        $this->pdo->enableZtd();

        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (2, 'Shadow', 100)");

        // Only shadow row visible
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM toggle_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM toggle_test WHERE id = 2');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow', $row['name']);
    }

    /**
     * Shadow INSERT, disable ZTD, verify physical empty, re-enable, shadow still there.
     */
    public function testShadowInsertVerifyPhysicalReenableShadow(): void
    {
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Shadow', 100)");

        // Disable — verify physical is empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM toggle_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Re-enable — shadow data still there
        $this->pdo->enableZtd();
        $stmt = $this->pdo->query('SELECT name FROM toggle_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow', $row['name']);
    }

    /**
     * Multiple toggle cycles with shadow operations in each enabled period.
     */
    public function testMultipleToggleCycles(): void
    {
        // Cycle 1: Insert shadow data
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'First', 10)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Cycle 2: Insert more shadow data
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (2, 'Second', 20)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Cycle 3: Update shadow data
        $this->pdo->exec('UPDATE toggle_test SET val = val + 100');

        // All shadow data should be present with updated values
        $stmt = $this->pdo->query('SELECT id, val FROM toggle_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(110, (int) $rows[0]['val']);
        $this->assertSame(120, (int) $rows[1]['val']);
    }

    /**
     * isZtdEnabled() reflects current state accurately.
     */
    public function testIsZtdEnabledAccuracy(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());

        $this->pdo->disableZtd();
        $this->assertFalse($this->pdo->isZtdEnabled());

        $this->pdo->enableZtd();
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    /**
     * Physical UPDATE while ZTD disabled doesn't affect shadow.
     */
    public function testPhysicalUpdateDoesntAffectShadow(): void
    {
        // First put data physically
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Physical', 50)");

        // Update physically
        $this->pdo->exec("UPDATE toggle_test SET name = 'Updated' WHERE id = 1");
        $this->pdo->enableZtd();

        // Shadow should be empty (doesn't see physical data)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM toggle_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow DELETE, then disable/enable cycle.
     */
    public function testShadowDeletePersistsAcrossToggle(): void
    {
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (1, 'Alice', 10)");
        $this->pdo->exec("INSERT INTO toggle_test (id, name, val) VALUES (2, 'Bob', 20)");
        $this->pdo->exec('DELETE FROM toggle_test WHERE id = 1');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Only Bob should remain in shadow
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM toggle_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM toggle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }
}
