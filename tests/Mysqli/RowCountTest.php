<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ztdAffectedRows() and lastAffectedRows() behavior after write operations
 * in ZTD mode on MySQLi. Uses ztdAffectedRows() for prepared statements,
 * lastAffectedRows() for query().
 * @spec SPEC-4.4
 */
class RowCountTest extends AbstractMysqliTestCase
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

    public function testAffectedRowsAfterUpdateMultiple(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_rc_items SET active = ? WHERE category = ?');
        $active = 0;
        $category = 'A';
        $stmt->bind_param('is', $active, $category);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testAffectedRowsAfterDeleteMultiple(): void
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

    public function testQueryReturnsAffectedRowCount(): void
    {
        $this->mysqli->query("UPDATE mi_rc_items SET active = 0 WHERE category = 'A'");
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testAffectedRowsAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_rc_items VALUES (5, 'Epsilon', 'C', 1)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());
    }
}
