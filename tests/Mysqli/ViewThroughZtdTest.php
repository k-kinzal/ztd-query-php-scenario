<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests database views behavior through ZTD via MySQLi.
 *
 * Cross-platform parity with MysqlViewThroughZtdTest (PDO).
 * @spec SPEC-3.3b
 */
class ViewThroughZtdTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_vtzt_users'];
    }


    /**
     * View returns physical data (not shadow).
     */
    public function testViewReturnsPhysicalData(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_active');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Shadow insert on base table not visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        $this->mysqli->query("INSERT INTO mi_vtzt_users VALUES (4, 'Diana', 1)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_active');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation of base table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_users');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }
}
