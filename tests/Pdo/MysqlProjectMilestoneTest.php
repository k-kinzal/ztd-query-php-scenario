<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a project milestone tracking workflow through ZTD shadow store (MySQL PDO).
 * Covers completion percentage via conditional COUNT, overdue milestone detection,
 * date-range filtering, project-at-risk identification via HAVING, and physical isolation.
 * @spec SPEC-10.2.87
 */
class MysqlProjectMilestoneTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_pm_projects (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                start_date DATETIME,
                target_date DATETIME,
                status VARCHAR(20)
            )',
            'CREATE TABLE mp_pm_milestones (
                id INT PRIMARY KEY,
                project_id INT,
                name VARCHAR(255),
                due_date DATETIME,
                completed_date DATETIME,
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_pm_milestones', 'mp_pm_projects'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 projects
        $this->pdo->exec("INSERT INTO mp_pm_projects VALUES (1, 'Website Redesign', '2026-01-01 00:00:00', '2026-06-30 00:00:00', 'active')");
        $this->pdo->exec("INSERT INTO mp_pm_projects VALUES (2, 'Mobile App', '2026-02-01 00:00:00', '2026-09-30 00:00:00', 'active')");

        // Milestones for Website Redesign (project 1)
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (1, 1, 'Wireframes', '2026-02-01 00:00:00', '2026-01-28 00:00:00', 'completed')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (2, 1, 'Design Mockups', '2026-03-01 00:00:00', '2026-02-25 00:00:00', 'completed')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (3, 1, 'Frontend Dev', '2026-03-05 00:00:00', NULL, 'overdue')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (4, 1, 'Backend Dev', '2026-04-15 00:00:00', NULL, 'pending')");

        // Milestones for Mobile App (project 2)
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (5, 2, 'Requirements', '2026-02-15 00:00:00', '2026-02-14 00:00:00', 'completed')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (6, 2, 'Prototype', '2026-03-01 00:00:00', NULL, 'overdue')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (7, 2, 'Alpha Release', '2026-05-01 00:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (8, 2, 'Beta Release', '2026-07-01 00:00:00', NULL, 'pending')");
    }

    /**
     * Project overview: JOIN projects+milestones, COUNT total and completed.
     */
    public function testProjectOverview(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    COUNT(m.id) AS total_milestones,
                    COUNT(CASE WHEN m.status = 'completed' THEN 1 END) AS completed_milestones
             FROM mp_pm_projects p
             JOIN mp_pm_milestones m ON m.project_id = p.id
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(2, $rows);

        // Website Redesign: 4 total, 2 completed
        $this->assertSame('Website Redesign', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['total_milestones']);
        $this->assertEquals(2, (int) $rows[0]['completed_milestones']);

        // Mobile App: 4 total, 1 completed
        $this->assertSame('Mobile App', $rows[1]['name']);
        $this->assertEquals(4, (int) $rows[1]['total_milestones']);
        $this->assertEquals(1, (int) $rows[1]['completed_milestones']);
    }

    /**
     * Completion percentage: COUNT(completed) * 100 / COUNT(*) GROUP BY project.
     */
    public function testCompletionPercentage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    COUNT(CASE WHEN m.status = 'completed' THEN 1 END) * 100 / COUNT(*) AS pct_complete
             FROM mp_pm_projects p
             JOIN mp_pm_milestones m ON m.project_id = p.id
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(2, $rows);

        // Website Redesign: 2/4 = 50%
        $this->assertSame('Website Redesign', $rows[0]['name']);
        $this->assertEquals(50, (int) $rows[0]['pct_complete']);

        // Mobile App: 1/4 = 25%
        $this->assertSame('Mobile App', $rows[1]['name']);
        $this->assertEquals(25, (int) $rows[1]['pct_complete']);
    }

    /**
     * Overdue milestones: due_date < '2026-03-09' AND status != 'completed', JOIN projects.
     */
    public function testOverdueMilestones(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name AS milestone_name, p.name AS project_name, m.due_date, m.status
             FROM mp_pm_milestones m
             JOIN mp_pm_projects p ON p.id = m.project_id
             WHERE m.due_date < '2026-03-09 00:00:00'
               AND m.status != 'completed'
             ORDER BY m.due_date"
        );

        // Frontend Dev (due 2026-03-05, overdue) and Prototype (due 2026-03-01, overdue)
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'milestone_name');
        $this->assertContains('Frontend Dev', $names);
        $this->assertContains('Prototype', $names);
    }

    /**
     * Complete a milestone: UPDATE status and completed_date, verify percentage changes.
     */
    public function testCompleteMilestone(): void
    {
        // Complete "Frontend Dev" (id=3)
        $affected = $this->pdo->exec("UPDATE mp_pm_milestones SET status = 'completed', completed_date = '2026-03-09 10:00:00' WHERE id = 3");
        $this->assertSame(1, $affected);

        // Verify the milestone is now completed
        $rows = $this->ztdQuery("SELECT status, completed_date FROM mp_pm_milestones WHERE id = 3");
        $this->assertSame('completed', $rows[0]['status']);
        $this->assertNotNull($rows[0]['completed_date']);

        // Verify Website Redesign completion is now 75% (3/4)
        $rows = $this->ztdQuery(
            "SELECT COUNT(CASE WHEN m.status = 'completed' THEN 1 END) * 100 / COUNT(*) AS pct_complete
             FROM mp_pm_milestones m
             WHERE m.project_id = 1"
        );
        $this->assertEquals(75, (int) $rows[0]['pct_complete']);
    }

    /**
     * Upcoming deadlines: BETWEEN date range, pending status, ORDER BY due_date, prepared.
     */
    public function testUpcomingDeadlines(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT m.name AS milestone_name, m.due_date, p.name AS project_name
             FROM mp_pm_milestones m
             JOIN mp_pm_projects p ON p.id = m.project_id
             WHERE m.due_date BETWEEN ? AND ?
               AND m.status = 'pending'
             ORDER BY m.due_date",
            ['2026-03-09 00:00:00', '2026-05-31 00:00:00']
        );

        // Backend Dev (2026-04-15) and Alpha Release (2026-05-01)
        $this->assertCount(2, $rows);
        $this->assertSame('Backend Dev', $rows[0]['milestone_name']);
        $this->assertSame('Alpha Release', $rows[1]['milestone_name']);
    }

    /**
     * Project at risk: projects with at least one overdue milestone via HAVING.
     */
    public function testProjectAtRisk(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, COUNT(CASE WHEN m.status = 'overdue' THEN 1 END) AS overdue_count
             FROM mp_pm_projects p
             JOIN mp_pm_milestones m ON m.project_id = p.id
             GROUP BY p.id, p.name
             HAVING COUNT(CASE WHEN m.status = 'overdue' THEN 1 END) > 0
             ORDER BY overdue_count DESC"
        );

        // Both projects have overdue milestones
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Website Redesign', $names);
        $this->assertContains('Mobile App', $names);

        // Each has exactly 1 overdue milestone
        $this->assertEquals(1, (int) $rows[0]['overdue_count']);
        $this->assertEquals(1, (int) $rows[1]['overdue_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_pm_milestones VALUES (9, 1, 'Launch', '2026-06-30 00:00:00', NULL, 'pending')");
        $this->pdo->exec("UPDATE mp_pm_projects SET status = 'completed' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_pm_milestones");
        $this->assertSame(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mp_pm_projects WHERE id = 1");
        $this->assertSame('completed', $rows[0]['status']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_pm_milestones")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
