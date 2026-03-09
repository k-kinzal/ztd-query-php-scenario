<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests NULL-safe comparison operators through MySQLi CTE shadow store.
 *
 * @spec SPEC-3.1, SPEC-10.1
 */
class MysqliNullSafeComparisonTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_nsc (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            manager_id INT,
            bonus DECIMAL(10,2),
            dept VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_nsc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_nsc VALUES (1, 'Alice', NULL, 100.00, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_nsc VALUES (2, 'Bob', 1, NULL, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_nsc VALUES (3, 'Carol', NULL, 200.00, NULL)");
        $this->mysqli->query("INSERT INTO mi_nsc VALUES (4, 'Dave', 1, 150.00, 'Sales')");
        $this->mysqli->query("INSERT INTO mi_nsc VALUES (5, 'Eve', NULL, NULL, NULL)");
    }

    /**
     * NULL <=> NULL should be TRUE.
     */
    public function testNullSafeEqualsBothNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM mi_nsc WHERE manager_id <=> bonus ORDER BY id"
        );
        $names = array_column($rows, 'name');
        $this->assertContains('Eve', $names);
    }

    /**
     * value <=> NULL should be FALSE for non-NULL values.
     */
    public function testNullSafeEqualsValueVsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM mi_nsc WHERE manager_id <=> NULL ORDER BY id"
        );
        $this->assertCount(3, $rows);
    }

    /**
     * COALESCE in WHERE clause.
     */
    public function testCoalesceInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_nsc WHERE COALESCE(bonus, 0) > 100 ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Carol', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * NULLIF function.
     */
    public function testNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, NULLIF(dept, 'Engineering') AS non_eng FROM mi_nsc ORDER BY id"
        );
        $this->assertNull($rows[0]['non_eng']);
        $this->assertSame('Sales', $rows[3]['non_eng']);
    }

    /**
     * IFNULL function.
     */
    public function testIfnull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, IFNULL(bonus, -1) AS val FROM mi_nsc ORDER BY id"
        );
        $this->assertEquals(-1, (float) $rows[1]['val']);
        $this->assertEquals(-1, (float) $rows[4]['val']);
    }

    /**
     * <=> with prepared statement.
     */
    public function testNullSafePrepared(): void
    {
        $stmt = $this->mysqli->prepare("SELECT name FROM mi_nsc WHERE dept <=> ? ORDER BY name");
        $val = null;
        $stmt->bind_param('s', $val);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_nsc');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
