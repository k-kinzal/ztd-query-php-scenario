<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests ZTD enable/disable toggle mid-session with data visibility transitions.
 * Verifies that shadow inserts survive toggle cycles, physical inserts are not
 * visible through ZTD, and shadow mutations persist across disable/re-enable.
 * @spec SPEC-10.2.95
 */
class ZtdTogglePatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_zt_messages (
            id INT PRIMARY KEY,
            sender VARCHAR(100),
            recipient VARCHAR(100),
            body VARCHAR(500),
            read_status TINYINT DEFAULT 0,
            sent_at DATETIME
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_zt_messages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert physical seed data while ZTD is disabled
        $this->disableZtd();
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query("INSERT INTO mi_zt_messages VALUES (1, 'Alice', 'Bob',   'Hello Bob',   0, '2026-03-09 10:00:00')");
        $raw->query("INSERT INTO mi_zt_messages VALUES (2, 'Bob',   'Alice', 'Hi Alice',    1, '2026-03-09 10:05:00')");
        $raw->query("INSERT INTO mi_zt_messages VALUES (3, 'Alice', 'Charlie', 'Hey there', 0, '2026-03-09 10:10:00')");
        $raw->close();
        $this->enableZtd();
    }

    /**
     * Shadow inserts are visible through ZTD, not in physical table.
     * @spec SPEC-10.2.95
     */
    public function testShadowInsertVisibleInZtd(): void
    {
        // Insert via ZTD (shadow)
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (4, 'Charlie', 'Alice', 'Reply', 0, '2026-03-09 11:00:00')");

        // ZTD sees shadow data (not physical data)
        $rows = $this->ztdQuery("SELECT id FROM mi_zt_messages WHERE id = 4");
        $this->assertCount(1, $rows);

        // Disable ZTD - physical table should have only the 3 physical rows, not the shadow insert
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zt_messages WHERE id = 4');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        // Re-enable - shadow data still accessible
        $this->enableZtd();
        $rows = $this->ztdQuery("SELECT sender FROM mi_zt_messages WHERE id = 4");
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
        // ZTD should show an empty shadow store (physical data is replaced)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages");
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }

    /**
     * Disabling ZTD shows only physical rows (not shadow inserts).
     * @spec SPEC-10.2.95
     */
    public function testDisableZtdSeesPhysicalOnly(): void
    {
        // Add a shadow insert
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (4, 'Test', 'User', 'Shadow msg', 0, '2026-03-09 12:00:00')");

        // ZTD sees the shadow insert
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages");
        $this->assertSame(1, (int) $rows[0]['cnt']);

        // Disable ZTD - sees only physical data
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zt_messages');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        $this->enableZtd();
    }

    /**
     * After disable+re-enable, shadow data is still accessible.
     * @spec SPEC-10.2.95
     */
    public function testReEnableRestoresShadow(): void
    {
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (10, 'Eve', 'Frank', 'Test', 0, '2026-03-09 13:00:00')");
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (11, 'Frank', 'Eve', 'Reply', 0, '2026-03-09 13:05:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $this->enableZtd();

        // Shadow data preserved across toggle cycle
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT sender FROM mi_zt_messages WHERE id = 10");
        $this->assertSame('Eve', $rows[0]['sender']);
    }

    /**
     * Physical INSERT (done with ZTD disabled) is visible only through physical
     * queries, not through ZTD queries.
     * @spec SPEC-10.2.95
     */
    public function testPhysicalInsertNotVisibleInZtd(): void
    {
        // Insert physical data with ZTD disabled
        $this->disableZtd();
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (20, 'Physical', 'User', 'Phys msg', 0, '2026-03-09 14:00:00')");

        // Physical query sees it
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zt_messages');
        $this->assertSame(4, (int) $result->fetch_assoc()['cnt']);

        // Re-enable ZTD - physical data is not visible through ZTD
        $this->enableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages WHERE id = 20");
        $this->assertSame(0, (int) $rows[0]['cnt']);

        // Clean up physical insert
        $this->disableZtd();
        $this->mysqli->query("DELETE FROM mi_zt_messages WHERE id = 20");
        $this->enableZtd();
    }

    /**
     * UPDATE a row via ZTD, disable/re-enable, verify update persists in shadow.
     * @spec SPEC-10.2.95
     */
    public function testShadowUpdateSurvivesToggle(): void
    {
        // Insert and update via ZTD
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (30, 'Grace', 'Hank', 'Original', 0, '2026-03-09 15:00:00')");
        $this->mysqli->query("UPDATE mi_zt_messages SET body = 'Updated', read_status = 1 WHERE id = 30");

        // Verify update in ZTD
        $rows = $this->ztdQuery("SELECT body, read_status FROM mi_zt_messages WHERE id = 30");
        $this->assertSame('Updated', $rows[0]['body']);
        $this->assertSame(1, (int) $rows[0]['read_status']);

        // Toggle
        $this->disableZtd();
        $this->enableZtd();

        // Update persists
        $rows = $this->ztdQuery("SELECT body, read_status FROM mi_zt_messages WHERE id = 30");
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
        // Perform various shadow mutations
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (40, 'Shadow1', 'User', 'Msg1', 0, '2026-03-09 16:00:00')");
        $this->mysqli->query("INSERT INTO mi_zt_messages VALUES (41, 'Shadow2', 'User', 'Msg2', 0, '2026-03-09 16:01:00')");
        $this->mysqli->query("UPDATE mi_zt_messages SET read_status = 1 WHERE id = 40");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_zt_messages");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        // Physical table has only the 3 original physical rows
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zt_messages');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        // None of the shadow IDs exist physically
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_zt_messages WHERE id IN (40, 41)');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
