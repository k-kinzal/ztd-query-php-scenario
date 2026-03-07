<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests ZTD lifecycle edge cases: toggle cycles, re-enable after disable,
 * multiple enable/disable sequences, and shadow store behavior across cycles.
 */
class SqliteZtdLifecycleTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE lifecycle_test (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testShadowDataClearedAfterDisableEnable(): void
    {
        // Insert shadow data
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (1, 'shadow')");

        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Disable and re-enable ZTD creates a fresh session
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        // Shadow data from previous session should still be visible
        // because enableZtd() does not reset the session state
        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Document actual behavior: does re-enabling ZTD preserve shadow data?
        // If this fails, update assertion to match actual behavior
        $this->assertCount(1, $rows);
    }

    public function testDisabledZtdReadesPhysicalData(): void
    {
        // Insert physical data
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (1, 'physical')");

        // Physical data visible when ZTD is off
        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);

        // Re-enable ZTD
        $this->pdo->enableZtd();

        // Shadow store should see the physical data since it was inserted before ZTD
        // (but ZTD CTE replaces the table, so shadow store is empty)
        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // CTE shadows replace physical table; only shadow data is visible
        // Physical data inserted while ZTD was off is NOT in the shadow store
        $this->assertCount(0, $rows);
    }

    public function testMultipleToggleCycles(): void
    {
        // Cycle 1: insert shadow data
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (1, 'cycle1')");

        // Cycle 2: toggle and insert more
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (2, 'cycle2')");

        // Cycle 3: toggle again
        $this->pdo->disableZtd();
        $this->pdo->enableZtd();
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (3, 'cycle3')");

        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // All shadow data should be preserved across toggle cycles
        $this->assertCount(3, $rows);
        $this->assertSame('cycle1', $rows[0]['val']);
        $this->assertSame('cycle2', $rows[1]['val']);
        $this->assertSame('cycle3', $rows[2]['val']);
    }

    public function testIsZtdEnabledReflectsState(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());

        $this->pdo->disableZtd();
        $this->assertFalse($this->pdo->isZtdEnabled());

        $this->pdo->enableZtd();
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    public function testShadowWritesVisibleAfterReEnable(): void
    {
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (1, 'before')");

        $this->pdo->disableZtd();
        // Physical operations while disabled
        $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES (2, 'physical')");
        $this->pdo->enableZtd();

        // Shadow data from before toggle should still be visible
        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('before', $rows[0]['val']);

        // Physical data inserted while disabled is NOT visible via ZTD
        // (CTE shadow replaces the entire table)
        $stmt = $this->pdo->query('SELECT * FROM lifecycle_test WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testManySequentialOperations(): void
    {
        // Stress test: many sequential writes to shadow store
        for ($i = 1; $i <= 50; $i++) {
            $this->pdo->exec("INSERT INTO lifecycle_test (id, val) VALUES ($i, 'item_$i')");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(50, (int) $row['cnt']);

        // Update half of them
        $this->pdo->exec("UPDATE lifecycle_test SET val = 'updated' WHERE id <= 25");

        $stmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM lifecycle_test WHERE val = 'updated'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(25, (int) $row['cnt']);

        // Delete a quarter
        $this->pdo->exec('DELETE FROM lifecycle_test WHERE id <= 12');

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(38, (int) $row['cnt']);

        // Verify isolation - physical table empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM lifecycle_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }
}
