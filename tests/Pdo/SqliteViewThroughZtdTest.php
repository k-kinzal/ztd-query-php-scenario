<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests database views behavior through ZTD on SQLite.
 *
 * Views are NOT rewritten by the CTE rewriter.
 * Querying a view returns empty results because no shadow data exists for views.
 */
class SqliteViewThroughZtdTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active INT)');
        $raw->exec('INSERT INTO vtzt_users VALUES (1, \'Alice\', 1)');
        $raw->exec('INSERT INTO vtzt_users VALUES (2, \'Bob\', 0)');
        $raw->exec('INSERT INTO vtzt_users VALUES (3, \'Charlie\', 1)');
        $raw->exec('CREATE VIEW vtzt_active_users AS SELECT id, name FROM vtzt_users WHERE active = 1');

        $this->pdo = ZtdPdo::fromPdo($raw);
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
