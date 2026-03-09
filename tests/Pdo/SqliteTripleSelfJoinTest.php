<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests same table joined to itself 3+ times with different aliases.
 * CTE rewriter must rewrite all references to the same table correctly.
 * @spec SPEC-10.2.99
 */
class SqliteTripleSelfJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_tsj_employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            manager_id INTEGER,
            department TEXT,
            salary REAL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_tsj_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // CEO -> 2 VPs -> 4 employees (tree structure)
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (1, 'Alice', NULL, 'Executive', 200000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (2, 'Bob', 1, 'Engineering', 150000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (3, 'Charlie', 1, 'Marketing', 140000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (4, 'Diana', 2, 'Engineering', 100000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (5, 'Eve', 2, 'Engineering', 95000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (6, 'Frank', 3, 'Marketing', 90000.00)");
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (7, 'Grace', 3, 'Marketing', 85000.00)");
    }

    /**
     * Triple self-join: employee -> manager -> grand-manager.
     */
    public function testTripleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, m.name AS manager, gm.name AS grand_manager
             FROM sl_tsj_employees e
             LEFT JOIN sl_tsj_employees m ON e.manager_id = m.id
             LEFT JOIN sl_tsj_employees gm ON m.manager_id = gm.id
             ORDER BY e.id"
        );

        $this->assertCount(7, $rows);

        // Alice (CEO): no manager, no grand-manager
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['manager']);
        $this->assertNull($rows[0]['grand_manager']);

        // Bob (VP): manager=Alice, grand-manager=NULL
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Alice', $rows[1]['manager']);
        $this->assertNull($rows[1]['grand_manager']);

        // Charlie (VP): manager=Alice, grand-manager=NULL
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertSame('Alice', $rows[2]['manager']);
        $this->assertNull($rows[2]['grand_manager']);

        // Diana: manager=Bob, grand-manager=Alice
        $this->assertSame('Diana', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['manager']);
        $this->assertSame('Alice', $rows[3]['grand_manager']);

        // Eve: manager=Bob, grand-manager=Alice
        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertSame('Bob', $rows[4]['manager']);
        $this->assertSame('Alice', $rows[4]['grand_manager']);

        // Frank: manager=Charlie, grand-manager=Alice
        $this->assertSame('Frank', $rows[5]['name']);
        $this->assertSame('Charlie', $rows[5]['manager']);
        $this->assertSame('Alice', $rows[5]['grand_manager']);

        // Grace: manager=Charlie, grand-manager=Alice
        $this->assertSame('Grace', $rows[6]['name']);
        $this->assertSame('Charlie', $rows[6]['manager']);
        $this->assertSame('Alice', $rows[6]['grand_manager']);
    }

    /**
     * Quadruple self-join: add great-grand-manager level (4 aliases of same table).
     */
    public function testQuadrupleSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    m.name AS manager,
                    gm.name AS grand_manager,
                    ggm.name AS great_grand_manager
             FROM sl_tsj_employees e
             LEFT JOIN sl_tsj_employees m ON e.manager_id = m.id
             LEFT JOIN sl_tsj_employees gm ON m.manager_id = gm.id
             LEFT JOIN sl_tsj_employees ggm ON gm.manager_id = ggm.id
             ORDER BY e.id"
        );

        $this->assertCount(7, $rows);

        // Diana: manager=Bob, grand=Alice, great-grand=NULL
        $this->assertSame('Diana', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['manager']);
        $this->assertSame('Alice', $rows[3]['grand_manager']);
        $this->assertNull($rows[3]['great_grand_manager']);

        // Alice: all NULL above her
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['manager']);
        $this->assertNull($rows[0]['grand_manager']);
        $this->assertNull($rows[0]['great_grand_manager']);
    }

    /**
     * Self-join with aggregate: count direct reports per manager.
     */
    public function testSelfJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, COUNT(e.id) AS reports
             FROM sl_tsj_employees e
             JOIN sl_tsj_employees m ON e.manager_id = m.id
             GROUP BY m.name
             ORDER BY reports DESC, m.name"
        );

        // Alice manages Bob, Charlie = 2 reports
        // Bob manages Diana, Eve = 2 reports
        // Charlie manages Frank, Grace = 2 reports
        $this->assertCount(3, $rows);
        $this->assertEquals(2, (int) $rows[0]['reports']);
        $this->assertEquals(2, (int) $rows[1]['reports']);
        $this->assertEquals(2, (int) $rows[2]['reports']);

        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Charlie', $names);
    }

    /**
     * Self-join with correlated subquery: count direct reports in SELECT list.
     */
    public function testSelfJoinWithSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    (SELECT COUNT(*) FROM sl_tsj_employees sub WHERE sub.manager_id = e.id) AS direct_reports
             FROM sl_tsj_employees e
             ORDER BY direct_reports DESC, e.name"
        );

        $this->assertCount(7, $rows);

        // Alice: 2, Bob: 2, Charlie: 2, Diana: 0, Eve: 0, Frank: 0, Grace: 0
        $byName = array_column($rows, 'direct_reports', 'name');
        $this->assertEquals(2, (int) $byName['Alice']);
        $this->assertEquals(2, (int) $byName['Bob']);
        $this->assertEquals(2, (int) $byName['Charlie']);
        $this->assertEquals(0, (int) $byName['Diana']);
        $this->assertEquals(0, (int) $byName['Eve']);
        $this->assertEquals(0, (int) $byName['Frank']);
        $this->assertEquals(0, (int) $byName['Grace']);
    }

    /**
     * Add new employee, verify self-join sees them.
     */
    public function testSelfJoinAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (8, 'Hank', 4, 'Engineering', 80000.00)");

        $rows = $this->ztdQuery(
            "SELECT e.name, m.name AS manager, gm.name AS grand_manager
             FROM sl_tsj_employees e
             LEFT JOIN sl_tsj_employees m ON e.manager_id = m.id
             LEFT JOIN sl_tsj_employees gm ON m.manager_id = gm.id
             WHERE e.name = 'Hank'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Hank', $rows[0]['name']);
        $this->assertSame('Diana', $rows[0]['manager']);
        $this->assertSame('Bob', $rows[0]['grand_manager']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_tsj_employees VALUES (8, 'Hank', 4, 'Engineering', 80000.00)");
        $this->pdo->exec("UPDATE sl_tsj_employees SET salary = 999999.00 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_tsj_employees");
        $this->assertEquals(8, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT salary FROM sl_tsj_employees WHERE id = 1");
        $this->assertEquals(999999.00, (float) $rows[0]['salary']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_tsj_employees')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
