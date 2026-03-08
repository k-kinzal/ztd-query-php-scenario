<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests database views behavior through ZTD on MySQL PDO.
 *
 * Views are NOT rewritten by the CTE rewriter — they pass through to physical DB.
 * @spec SPEC-3.3b
 */
class MysqlViewThroughZtdTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_vtzt_users'];
    }


    /**
     * View returns physical data (not shadow).
     */
    public function testViewReturnsPhysicalData(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow insert on base table not visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        $this->pdo->exec("INSERT INTO pdo_vtzt_users VALUES (4, 'Diana', 1)");

        // View still reads physical data
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation of base table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }
}
