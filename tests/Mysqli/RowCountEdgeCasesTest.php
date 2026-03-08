<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ztdAffectedRows() and affected_rows behavior after write operations in ZTD mode on MySQLi.
 * @spec SPEC-4.4
 */
class RowCountEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_rc_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), active TINYINT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_rc_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (1, 'Alpha', 'A', 1)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (2, 'Beta', 'A', 1)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (3, 'Gamma', 'B', 0)");
        $this->mysqli->query("INSERT INTO mi_rc_items (id, name, category, active) VALUES (4, 'Delta', 'B', 1)");
    }

    public function testAffectedRowsAfterPreparedUpdateMultiple(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_rc_items SET active = ? WHERE category = ?');
        $active = 0;
        $category = 'A';
        $stmt->bind_param('is', $active, $category);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsAfterPreparedDeleteMultiple(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_rc_items WHERE category = ?');
        $category = 'B';
        $stmt->bind_param('s', $category);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsAfterUpdateNoMatch(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_rc_items SET name = ? WHERE id = ?');
        $name = 'NoMatch';
        $id = 999;
        $stmt->bind_param('si', $name, $id);
        $stmt->execute();
        $this->assertSame(0, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsReExecuteWithFrozenSnapshot(): void
    {
        $stmt = $this->mysqli->prepare('DELETE FROM mi_rc_items WHERE category = ?');
        $category = 'A';
        $stmt->bind_param('s', $category);

        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());

        $category = 'B';
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());

        // CTE snapshot is frozen at prepare time — DELETE still "sees" 'A'
        // rows in the snapshot even though they were already deleted.
        $category = 'A';
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }
}
