<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ZTD enable/disable toggle mid-session with data visibility transitions.
 * Verifies that shadow inserts survive toggle cycles, physical inserts are not
 * visible through ZTD, and shadow mutations persist across disable/re-enable.
 *
 * Note: SQLite uses in-memory databases, so physical seed data must be inserted
 * via the same PDO connection with ZTD disabled.
 * @spec SPEC-10.2.95
 */
class SqliteZtdTogglePatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_zt_messages (
            id INTEGER PRIMARY KEY,
            sender TEXT,
            recipient TEXT,
            body TEXT,
            read_status INTEGER DEFAULT 0,
            sent_at TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_zt_messages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert physical seed data with ZTD disabled
        $this->disableZtd();
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (1, 'Alice', 'Bob',   'Hello Bob',   0, '2026-03-09 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (2, 'Bob',   'Alice', 'Hi Alice',    1, '2026-03-09 10:05:00')");
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (3, 'Alice', 'Charlie', 'Hey there', 0, '2026-03-09 10:10:00')");
        $this->enableZtd();
    }

    /**
     * Shadow inserts are visible through ZTD, not in physical table.
     * @spec SPEC-10.2.95
     */
    public function testShadowInsertVisibleInZtd(): void
    {
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (4, 'Charlie', 'Alice', 'Reply', 0, '2026-03-09 11:00:00')");

        $rows = $this->ztdQuery("SELECT id FROM sl_zt_messages WHERE id = 4");
        $this->assertCount(1, $rows);

        // Disable ZTD - physical table has only the 3 physical rows
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_zt_messages WHERE id = 4')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Re-enable - shadow data still accessible
        $this->enableZtd();
        $rows = $this->ztdQuery("SELECT sender FROM sl_zt_messages WHERE id = 4");
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['sender']);
    }

    /**
     * Physical data (inserted with ZTD disabled) is NOT visible through ZTD.
     * ZTD replaces the physical table with the shadow store.
     * @spec SPEC-10.2.95
     */
    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages");
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }

    /**
     * Disabling ZTD shows only physical rows (not shadow inserts).
     * @spec SPEC-10.2.95
     */
    public function testDisableZtdSeesPhysicalOnly(): void
    {
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (4, 'Test', 'User', 'Shadow msg', 0, '2026-03-09 12:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages");
        $this->assertSame(1, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_zt_messages')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $this->enableZtd();
    }

    /**
     * After disable+re-enable, shadow data is still accessible.
     * @spec SPEC-10.2.95
     */
    public function testReEnableRestoresShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (10, 'Eve', 'Frank', 'Test', 0, '2026-03-09 13:00:00')");
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (11, 'Frank', 'Eve', 'Reply', 0, '2026-03-09 13:05:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $this->enableZtd();

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT sender FROM sl_zt_messages WHERE id = 10");
        $this->assertSame('Eve', $rows[0]['sender']);
    }

    /**
     * Physical INSERT (done with ZTD disabled) is visible only through physical
     * queries, not through ZTD queries.
     * @spec SPEC-10.2.95
     */
    public function testPhysicalInsertNotVisibleInZtd(): void
    {
        $this->disableZtd();
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (20, 'Physical', 'User', 'Phys msg', 0, '2026-03-09 14:00:00')");

        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_zt_messages')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $rows[0]['cnt']);

        $this->enableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages WHERE id = 20");
        $this->assertSame(0, (int) $rows[0]['cnt']);

        // Clean up
        $this->disableZtd();
        $this->pdo->exec("DELETE FROM sl_zt_messages WHERE id = 20");
        $this->enableZtd();
    }

    /**
     * UPDATE a row via ZTD, disable/re-enable, verify update persists in shadow.
     * @spec SPEC-10.2.95
     */
    public function testShadowUpdateSurvivesToggle(): void
    {
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (30, 'Grace', 'Hank', 'Original', 0, '2026-03-09 15:00:00')");
        $this->pdo->exec("UPDATE sl_zt_messages SET body = 'Updated', read_status = 1 WHERE id = 30");

        $rows = $this->ztdQuery("SELECT body, read_status FROM sl_zt_messages WHERE id = 30");
        $this->assertSame('Updated', $rows[0]['body']);
        $this->assertSame(1, (int) $rows[0]['read_status']);

        $this->disableZtd();
        $this->enableZtd();

        $rows = $this->ztdQuery("SELECT body, read_status FROM sl_zt_messages WHERE id = 30");
        $this->assertCount(1, $rows);
        $this->assertSame('Updated', $rows[0]['body']);
        $this->assertSame(1, (int) $rows[0]['read_status']);
    }

    /**
     * Verify all shadow mutations didn't reach the physical table.
     * @spec SPEC-10.2.95
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (40, 'Shadow1', 'User', 'Msg1', 0, '2026-03-09 16:00:00')");
        $this->pdo->exec("INSERT INTO sl_zt_messages VALUES (41, 'Shadow2', 'User', 'Msg2', 0, '2026-03-09 16:01:00')");
        $this->pdo->exec("UPDATE sl_zt_messages SET read_status = 1 WHERE id = 40");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_zt_messages')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_zt_messages WHERE id IN (40, 41)')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
