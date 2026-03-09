<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests an employee onboarding checklist scenario through ZTD shadow store (PostgreSQL PDO).
 * Departments have employees, each employee must complete a set of checklist items.
 * SQL patterns exercised: LEFT JOIN with COUNT/SUM for completion tracking,
 * ROUND percentage, NOT EXISTS for outstanding items, UPDATE followed by
 * SELECT verification, GROUP BY department for progress summary,
 * CASE for completion status labels, prepared statement for employee lookup.
 * @spec SPEC-10.2.151
 */
class PostgresOnboardingChecklistTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ob_departments (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            )',
            'CREATE TABLE pg_ob_employees (
                id SERIAL PRIMARY KEY,
                department_id INTEGER,
                name VARCHAR(100),
                start_date TEXT
            )',
            'CREATE TABLE pg_ob_checklist_items (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                category VARCHAR(50)
            )',
            'CREATE TABLE pg_ob_completions (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER,
                item_id INTEGER,
                completed_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ob_completions', 'pg_ob_checklist_items', 'pg_ob_employees', 'pg_ob_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Departments
        $this->pdo->exec("INSERT INTO pg_ob_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_ob_departments VALUES (2, 'Marketing')");

        // Employees
        $this->pdo->exec("INSERT INTO pg_ob_employees VALUES (1, 1, 'Alice', '2026-01-15')");
        $this->pdo->exec("INSERT INTO pg_ob_employees VALUES (2, 1, 'Bob', '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_ob_employees VALUES (3, 2, 'Carol', '2026-01-20')");
        $this->pdo->exec("INSERT INTO pg_ob_employees VALUES (4, 2, 'Dave', '2026-03-01')");

        // Checklist items (5 items every employee must complete)
        $this->pdo->exec("INSERT INTO pg_ob_checklist_items VALUES (1, 'Sign employment contract', 'admin')");
        $this->pdo->exec("INSERT INTO pg_ob_checklist_items VALUES (2, 'Set up workstation', 'setup')");
        $this->pdo->exec("INSERT INTO pg_ob_checklist_items VALUES (3, 'Complete security training', 'training')");
        $this->pdo->exec("INSERT INTO pg_ob_checklist_items VALUES (4, 'Meet with manager', 'intro')");
        $this->pdo->exec("INSERT INTO pg_ob_checklist_items VALUES (5, 'Review company handbook', 'training')");

        // Completions: Alice 5/5, Bob 3/5, Carol 4/5, Dave 1/5
        // Alice: all done
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (1, 1, 1, '2026-01-15')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (2, 1, 2, '2026-01-15')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (3, 1, 3, '2026-01-16')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (4, 1, 4, '2026-01-16')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (5, 1, 5, '2026-01-17')");

        // Bob: items 1, 2, 4 done (missing 3: security training, 5: handbook)
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (6, 2, 1, '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (7, 2, 2, '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (8, 2, 4, '2026-02-02')");

        // Carol: items 1, 2, 3, 5 done (missing 4: meet with manager)
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (9, 3, 1, '2026-01-20')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (10, 3, 2, '2026-01-20')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (11, 3, 3, '2026-01-21')");
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (12, 3, 5, '2026-01-22')");

        // Dave: item 1 only
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (13, 4, 1, '2026-03-01')");
    }

    /**
     * LEFT JOIN + COUNT for completion progress per employee.
     * Uses CROSS JOIN to get all employee-item combinations, then LEFT JOIN completions.
     * Alice=100%, Bob=60%, Carol=80%, Dave=20%.
     */
    public function testCompletionProgressPerEmployee(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    COUNT(c.id) AS completed,
                    (SELECT COUNT(*) FROM pg_ob_checklist_items) AS total,
                    ROUND(COUNT(c.id) * 100.0 / (SELECT COUNT(*) FROM pg_ob_checklist_items)) AS pct
             FROM pg_ob_employees e
             LEFT JOIN pg_ob_completions c ON c.employee_id = e.id
             GROUP BY e.id, e.name
             ORDER BY pct DESC, e.name ASC"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(100, (int) $rows[0]['pct']);

        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertEquals(80, (int) $rows[1]['pct']);

        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(60, (int) $rows[2]['pct']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEquals(20, (int) $rows[3]['pct']);
    }

    /**
     * NOT EXISTS: find outstanding items for a specific employee.
     * Bob is missing items 3 (security training) and 5 (handbook).
     */
    public function testOutstandingItemsForEmployee(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ci.title, ci.category
             FROM pg_ob_checklist_items ci
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_ob_completions c
                 WHERE c.item_id = ci.id AND c.employee_id = 2
             )
             ORDER BY ci.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Complete security training', $rows[0]['title']);
        $this->assertSame('training', $rows[0]['category']);
        $this->assertSame('Review company handbook', $rows[1]['title']);
        $this->assertSame('training', $rows[1]['category']);
    }

    /**
     * GROUP BY department: average completion percentage per department.
     * Engineering: (100+60)/2 = 80%. Marketing: (80+20)/2 = 50%.
     */
    public function testDepartmentProgress(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name AS department,
                    COUNT(DISTINCT e.id) AS employee_count,
                    ROUND(AVG(sub.pct)) AS avg_completion
             FROM pg_ob_departments d
             JOIN pg_ob_employees e ON e.department_id = d.id
             JOIN (
                 SELECT e2.id AS emp_id,
                        COUNT(c.id) * 100.0 / (SELECT COUNT(*) FROM pg_ob_checklist_items) AS pct
                 FROM pg_ob_employees e2
                 LEFT JOIN pg_ob_completions c ON c.employee_id = e2.id
                 GROUP BY e2.id
             ) sub ON sub.emp_id = e.id
             GROUP BY d.id, d.name
             ORDER BY d.name"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertEquals(2, (int) $rows[0]['employee_count']);
        $this->assertEquals(80, (int) $rows[0]['avg_completion']);

        $this->assertSame('Marketing', $rows[1]['department']);
        $this->assertEquals(2, (int) $rows[1]['employee_count']);
        $this->assertEquals(50, (int) $rows[1]['avg_completion']);
    }

    /**
     * CASE for completion status labels: 100% = 'complete', >= 50% = 'in_progress', < 50% = 'behind'.
     */
    public function testCompletionStatusLabels(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    CASE
                        WHEN COUNT(c.id) * 100 / (SELECT COUNT(*) FROM pg_ob_checklist_items) = 100 THEN 'complete'
                        WHEN COUNT(c.id) * 100 / (SELECT COUNT(*) FROM pg_ob_checklist_items) >= 50 THEN 'in_progress'
                        ELSE 'behind'
                    END AS status
             FROM pg_ob_employees e
             LEFT JOIN pg_ob_completions c ON c.employee_id = e.id
             GROUP BY e.id, e.name
             ORDER BY e.name"
        );

        $this->assertCount(4, $rows);

        $statuses = [];
        foreach ($rows as $row) {
            $statuses[$row['name']] = $row['status'];
        }

        $this->assertSame('complete', $statuses['Alice']);
        $this->assertSame('in_progress', $statuses['Bob']);
        $this->assertSame('in_progress', $statuses['Carol']);
        $this->assertSame('behind', $statuses['Dave']);
    }

    /**
     * UPDATE + SELECT verification: mark an item complete, then verify progress changed.
     */
    public function testMarkItemCompleteAndVerify(): void
    {
        // Before: Bob has 3/5 = 60%
        $rows = $this->ztdQuery(
            "SELECT COUNT(c.id) AS completed
             FROM pg_ob_completions c
             WHERE c.employee_id = 2"
        );
        $this->assertEquals(3, (int) $rows[0]['completed']);

        // Mark security training as complete for Bob
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (14, 2, 3, '2026-03-09')");

        // After: Bob has 4/5 = 80%
        $rows = $this->ztdQuery(
            "SELECT COUNT(c.id) AS completed,
                    ROUND(COUNT(c.id) * 100.0 / (SELECT COUNT(*) FROM pg_ob_checklist_items)) AS pct
             FROM pg_ob_completions c
             WHERE c.employee_id = 2"
        );
        $this->assertEquals(4, (int) $rows[0]['completed']);
        $this->assertEquals(80, (int) $rows[0]['pct']);

        // Only one item remaining for Bob
        $rows = $this->ztdQuery(
            "SELECT ci.title
             FROM pg_ob_checklist_items ci
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_ob_completions c
                 WHERE c.item_id = ci.id AND c.employee_id = 2
             )"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Review company handbook', $rows[0]['title']);
    }

    /**
     * COUNT GROUP BY category: completion counts by category across all employees.
     */
    public function testCompletionByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ci.category,
                    COUNT(DISTINCT ci.id) AS items_in_category,
                    COUNT(c.id) AS total_completions
             FROM pg_ob_checklist_items ci
             LEFT JOIN pg_ob_completions c ON c.item_id = ci.id
             GROUP BY ci.category
             ORDER BY ci.category"
        );

        $this->assertCount(4, $rows);

        $categories = [];
        foreach ($rows as $row) {
            $categories[$row['category']] = [
                'items' => (int) $row['items_in_category'],
                'completions' => (int) $row['total_completions'],
            ];
        }

        // admin (1 item: contract): all 4 employees completed = 4
        $this->assertEquals(1, $categories['admin']['items']);
        $this->assertEquals(4, $categories['admin']['completions']);

        // intro (1 item: meet manager): Alice, Bob, not Carol, not Dave = 2... wait
        // Actually: Alice(4), Bob(4), Carol missing(4), Dave missing
        // Item 4 (meet manager): Alice yes, Bob yes, Carol NO, Dave NO => 2 completions
        $this->assertEquals(1, $categories['intro']['items']);
        $this->assertEquals(2, $categories['intro']['completions']);

        // setup (1 item: workstation): Alice, Bob, Carol, not Dave = 3
        $this->assertEquals(1, $categories['setup']['items']);
        $this->assertEquals(3, $categories['setup']['completions']);

        // training (2 items: security + handbook): various completions
        // Item 3 (security): Alice, Carol = 2
        // Item 5 (handbook): Alice, Carol = 2
        // Total = 4
        $this->assertEquals(2, $categories['training']['items']);
        $this->assertEquals(4, $categories['training']['completions']);
    }

    /**
     * Prepared statement: lookup employee's completion details by department.
     */
    public function testPreparedCompletionByDepartment(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name,
                    COUNT(c.id) AS completed,
                    (SELECT COUNT(*) FROM pg_ob_checklist_items) AS total
             FROM pg_ob_employees e
             LEFT JOIN pg_ob_completions c ON c.employee_id = e.id
             WHERE e.department_id = ?
             GROUP BY e.id, e.name
             ORDER BY e.name",
            [1]  // Engineering
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(5, (int) $rows[0]['completed']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['completed']);
    }

    /**
     * Physical isolation: completions inserted through ZTD stay in shadow store.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ob_completions VALUES (14, 4, 2, '2026-03-09')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ob_completions");
        $this->assertEquals(14, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_ob_completions')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
