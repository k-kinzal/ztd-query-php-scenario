<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests database views behavior through ZTD on SQLite.
 *
 * Views are NOT rewritten by the CTE rewriter.
 * Querying a view returns empty results because no shadow data exists for views.
 * @spec SPEC-3.3b
 */
class SqliteViewThroughZtdTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active INT)';
    }

    protected function getTableNames(): array
    {
        return ['vtzt_users'];
    }


    /**
     * View query returns physical data (not shadow) because views are not rewritten.
     */
    public function testViewReturnsPhysicalData(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM vtzt_active_users');
        $count = (int) $stmt->fetchColumn();
        // Views pass through to physical DB — should see 2 active users
        $this->assertSame(2, $count);
    }

    /**
     * Shadow mutations on base table are NOT visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        // Insert via shadow
        $this->pdo->exec("INSERT INTO vtzt_users VALUES (4, 'Diana', 1)");

        // Shadow SELECT on base table sees only shadow data (1 row, not physical)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM vtzt_users');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        // View does NOT see shadow insert — reads from physical table directly
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM vtzt_active_users');
        $count = (int) $stmt->fetchColumn();
        // View reads physical data: still 2 active users
        $this->assertSame(2, $count);
    }

    /**
     * Physical isolation: base table has physical data even when ZTD disabled.
     */
    public function testBaseTableHasPhysicalData(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM vtzt_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM vtzt_active_users');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }
}
