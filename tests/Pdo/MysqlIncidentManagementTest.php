<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an IT incident management scenario through ZTD shadow store (MySQL PDO).
 * Incidents are triaged by severity, assigned to agents, and tracked through
 * resolution. Exercises GROUP BY with COUNT, CASE expressions for priority
 * labeling, LEFT JOIN with COUNT/COUNT DISTINCT for workload, IS NOT NULL
 * filtering, NOT EXISTS subquery, prepared statement with JOIN,
 * and physical isolation check.
 * @spec SPEC-10.2.160
 */
class MysqlIncidentManagementTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_im_incidents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                severity VARCHAR(255),
                status VARCHAR(255),
                reported_at TEXT,
                resolved_at TEXT,
                reporter VARCHAR(255)
            )',
            'CREATE TABLE mp_im_agents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                team VARCHAR(255),
                tier INT
            )',
            'CREATE TABLE mp_im_assignments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                incident_id INT,
                agent_id INT,
                assigned_at TEXT,
                action VARCHAR(255),
                notes TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_im_assignments', 'mp_im_agents', 'mp_im_incidents'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 6 incidents
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (1, 'Database connection timeout', 'critical', 'resolved', '2026-02-10', '2026-02-10', 'user_a')");
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (2, 'Login page 500 error', 'high', 'investigating', '2026-02-28', NULL, 'user_b')");
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (3, 'Slow report generation', 'medium', 'open', '2026-03-01', NULL, 'user_c')");
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (4, 'Email notifications delayed', 'low', 'resolved', '2026-02-15', '2026-02-17', 'user_a')");
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (5, 'Payment gateway failure', 'critical', 'closed', '2026-01-20', '2026-01-20', 'user_d')");
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (6, 'Dashboard widget broken', 'medium', 'open', '2026-03-08', NULL, 'user_b')");

        // 4 agents
        $this->pdo->exec("INSERT INTO mp_im_agents VALUES (1, 'Sarah', 'infrastructure', 1)");
        $this->pdo->exec("INSERT INTO mp_im_agents VALUES (2, 'James', 'application', 1)");
        $this->pdo->exec("INSERT INTO mp_im_agents VALUES (3, 'Li', 'infrastructure', 2)");
        $this->pdo->exec("INSERT INTO mp_im_agents VALUES (4, 'Nina', 'application', 2)");

        // 8 assignments
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (1, 1, 1, '2026-02-10 08:30', 'investigate', 'Checking connection pool')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (2, 1, 3, '2026-02-10 09:00', 'escalate', 'DB server OOM - restarted')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (3, 1, 1, '2026-02-10 10:00', 'resolve', 'Connection pool tuned')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (4, 2, 2, '2026-02-28 14:00', 'investigate', 'Reproducing on staging')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (5, 4, 4, '2026-02-15 10:00', 'investigate', 'Checking SMTP config')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (6, 4, 4, '2026-02-17 09:00', 'resolve', 'SMTP timeout increased')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (7, 5, 1, '2026-01-20 03:00', 'investigate', 'Gateway cert expired')");
        $this->pdo->exec("INSERT INTO mp_im_assignments VALUES (8, 5, 3, '2026-01-20 04:00', 'resolve', 'Certificate renewed')");
    }

    /**
     * GROUP BY severity with COUNT, ORDER BY severity.
     * Expected: critical=2, high=1, low=1, medium=2.
     */
    public function testIncidentCountBySeverity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.severity, COUNT(*) AS cnt
             FROM mp_im_incidents i
             GROUP BY i.severity
             ORDER BY i.severity"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('critical', $rows[0]['severity']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $this->assertSame('high', $rows[1]['severity']);
        $this->assertEquals(1, (int) $rows[1]['cnt']);

        $this->assertSame('low', $rows[2]['severity']);
        $this->assertEquals(1, (int) $rows[2]['cnt']);

        $this->assertSame('medium', $rows[3]['severity']);
        $this->assertEquals(2, (int) $rows[3]['cnt']);
    }

    /**
     * Open/investigating incidents with CASE severity for priority label.
     * Expected 3 rows: Login page 500 error (P2), Slow report generation (P3),
     * Dashboard widget broken (P3).
     */
    public function testOpenIncidentsByPriority(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.title, i.severity,
                    CASE i.severity
                        WHEN 'critical' THEN 'P1'
                        WHEN 'high' THEN 'P2'
                        WHEN 'medium' THEN 'P3'
                        WHEN 'low' THEN 'P4'
                    END AS priority
             FROM mp_im_incidents i
             WHERE i.status IN ('open', 'investigating')
             ORDER BY CASE i.severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END, i.id"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Login page 500 error', $rows[0]['title']);
        $this->assertSame('high', $rows[0]['severity']);
        $this->assertSame('P2', $rows[0]['priority']);

        $this->assertSame('Slow report generation', $rows[1]['title']);
        $this->assertSame('medium', $rows[1]['severity']);
        $this->assertSame('P3', $rows[1]['priority']);

        $this->assertSame('Dashboard widget broken', $rows[2]['title']);
        $this->assertSame('medium', $rows[2]['severity']);
        $this->assertSame('P3', $rows[2]['priority']);
    }

    /**
     * Agent workload: LEFT JOIN agents to assignments, COUNT total actions,
     * COUNT DISTINCT incident_id for unique incidents.
     * Expected: James=1/1, Li=2/2, Nina=2/1, Sarah=3/2.
     */
    public function testAgentWorkload(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name, COUNT(asgn.id) AS total_actions,
                    COUNT(DISTINCT asgn.incident_id) AS unique_incidents
             FROM mp_im_agents a
             LEFT JOIN mp_im_assignments asgn ON asgn.agent_id = a.id
             GROUP BY a.id, a.name
             ORDER BY a.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('James', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['total_actions']);
        $this->assertEquals(1, (int) $rows[0]['unique_incidents']);

        $this->assertSame('Li', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['total_actions']);
        $this->assertEquals(2, (int) $rows[1]['unique_incidents']);

        $this->assertSame('Nina', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['total_actions']);
        $this->assertEquals(1, (int) $rows[2]['unique_incidents']);

        $this->assertSame('Sarah', $rows[3]['name']);
        $this->assertEquals(3, (int) $rows[3]['total_actions']);
        $this->assertEquals(2, (int) $rows[3]['unique_incidents']);
    }

    /**
     * Resolution time summary: resolved incidents where resolved_at IS NOT NULL.
     * Expected 3 rows ordered by reported_at:
     * Payment gateway failure, Database connection timeout, Email notifications delayed.
     */
    public function testResolutionTimeSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.title, i.reported_at, i.resolved_at
             FROM mp_im_incidents i
             WHERE i.resolved_at IS NOT NULL
             ORDER BY i.reported_at"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Payment gateway failure', $rows[0]['title']);
        $this->assertSame('2026-01-20', $rows[0]['reported_at']);
        $this->assertSame('2026-01-20', $rows[0]['resolved_at']);

        $this->assertSame('Database connection timeout', $rows[1]['title']);
        $this->assertSame('2026-02-10', $rows[1]['reported_at']);
        $this->assertSame('2026-02-10', $rows[1]['resolved_at']);

        $this->assertSame('Email notifications delayed', $rows[2]['title']);
        $this->assertSame('2026-02-15', $rows[2]['reported_at']);
        $this->assertSame('2026-02-17', $rows[2]['resolved_at']);
    }

    /**
     * NOT EXISTS subquery: incidents with no assignments.
     * Expected 2 rows: Slow report generation (medium), Dashboard widget broken (medium).
     */
    public function testUnassignedIncidents(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.title, i.severity
             FROM mp_im_incidents i
             WHERE NOT EXISTS (SELECT 1 FROM mp_im_assignments asgn WHERE asgn.incident_id = i.id)
             ORDER BY i.id"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Slow report generation', $rows[0]['title']);
        $this->assertSame('medium', $rows[0]['severity']);

        $this->assertSame('Dashboard widget broken', $rows[1]['title']);
        $this->assertSame('medium', $rows[1]['severity']);
    }

    /**
     * Prepared statement: all assignments for a given team, JOIN incidents.
     * Params: ['infrastructure'] → agents Sarah(1) and Li(3).
     * Expected 2 distinct incidents: Database connection timeout, Payment gateway failure.
     */
    public function testPreparedTeamIncidents(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT DISTINCT i.title, i.severity, i.status
             FROM mp_im_assignments asgn
             JOIN mp_im_agents a ON a.id = asgn.agent_id
             JOIN mp_im_incidents i ON i.id = asgn.incident_id
             WHERE a.team = ?
             ORDER BY i.id",
            ['infrastructure']
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Database connection timeout', $rows[0]['title']);
        $this->assertSame('critical', $rows[0]['severity']);
        $this->assertSame('resolved', $rows[0]['status']);

        $this->assertSame('Payment gateway failure', $rows[1]['title']);
        $this->assertSame('critical', $rows[1]['severity']);
        $this->assertSame('closed', $rows[1]['status']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new incident via shadow
        $this->pdo->exec("INSERT INTO mp_im_incidents VALUES (7, 'Test incident', 'low', 'open', '2026-03-09', NULL, 'user_e')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_im_incidents");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_im_incidents")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
