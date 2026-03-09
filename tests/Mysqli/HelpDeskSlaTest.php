<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a help desk SLA tracking scenario: MIN correlated subquery for first response time,
 * CASE-based SLA evaluation, LEFT JOIN metrics, and date string comparison (MySQLi).
 * @spec SPEC-10.2.126
 */
class HelpDeskSlaTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_hd_tickets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                subject VARCHAR(255),
                priority VARCHAR(50),
                status VARCHAR(50),
                created_at VARCHAR(50),
                assigned_agent VARCHAR(100)
            )',
            'CREATE TABLE mi_hd_responses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticket_id INT,
                responder VARCHAR(100),
                response_at VARCHAR(50),
                is_internal INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_hd_responses', 'mi_hd_tickets'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Tickets
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (1, 'Login broken', 'critical', 'resolved', '2025-10-01 08:00', 'agent_a')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (2, 'Slow dashboard', 'high', 'open', '2025-10-01 09:30', 'agent_a')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (3, 'Feature request', 'low', 'open', '2025-10-01 10:00', 'agent_b')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (4, 'Data export fail', 'critical', 'in_progress', '2025-10-02 07:00', 'agent_b')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (5, 'UI glitch', 'medium', 'resolved', '2025-10-02 11:00', 'agent_c')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (6, 'Password reset', 'high', 'resolved', '2025-10-03 06:00', 'agent_a')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (7, 'API timeout', 'critical', 'open', '2025-10-03 14:00', 'agent_c')");
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (8, 'Billing question', 'low', 'resolved', '2025-10-04 09:00', NULL)");

        // Responses
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (1, 1, 'agent_a', '2025-10-01 08:15', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (2, 1, 'agent_a', '2025-10-01 09:00', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (3, 2, 'agent_a', '2025-10-01 11:00', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (4, 2, 'agent_a', '2025-10-01 11:30', 1)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (5, 4, 'agent_b', '2025-10-02 07:30', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (6, 4, 'agent_b', '2025-10-02 08:00', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (7, 5, 'agent_c', '2025-10-02 11:20', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (8, 5, 'agent_c', '2025-10-02 12:00', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (9, 6, 'agent_a', '2025-10-03 06:10', 0)");
        $this->mysqli->query("INSERT INTO mi_hd_responses VALUES (10, 6, 'agent_a', '2025-10-03 06:30', 0)");
    }

    /**
     * First response time per ticket using MIN with LEFT JOIN (excludes internal notes).
     */
    public function testFirstResponseTime(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.subject, t.priority,
                    MIN(r.response_at) AS first_response
             FROM mi_hd_tickets t
             LEFT JOIN mi_hd_responses r ON r.ticket_id = t.id AND r.is_internal = 0
             GROUP BY t.id, t.subject, t.priority
             ORDER BY t.id"
        );

        $this->assertCount(8, $rows);

        // Ticket 1: first response at 08:15
        $this->assertSame('2025-10-01 08:15', $rows[0]['first_response']);

        // Ticket 3: no responses
        $this->assertNull($rows[2]['first_response']);

        // Ticket 8: no responses
        $this->assertNull($rows[7]['first_response']);
    }

    /**
     * SLA breach evaluation using CASE: tickets with no response vs responded.
     */
    public function testSlaBreach(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.subject, t.priority,
                    MIN(r.response_at) AS first_response,
                    CASE WHEN MIN(r.response_at) IS NULL THEN 'no_response'
                         ELSE 'responded'
                    END AS response_status
             FROM mi_hd_tickets t
             LEFT JOIN mi_hd_responses r ON r.ticket_id = t.id AND r.is_internal = 0
             GROUP BY t.id, t.subject, t.priority
             ORDER BY t.id"
        );

        $this->assertCount(8, $rows);

        // Tickets with responses
        $this->assertSame('responded', $rows[0]['response_status']); // ticket 1
        $this->assertSame('responded', $rows[1]['response_status']); // ticket 2
        $this->assertSame('responded', $rows[3]['response_status']); // ticket 4
        $this->assertSame('responded', $rows[4]['response_status']); // ticket 5
        $this->assertSame('responded', $rows[5]['response_status']); // ticket 6

        // Tickets without responses
        $this->assertSame('no_response', $rows[2]['response_status']); // ticket 3
        $this->assertSame('no_response', $rows[6]['response_status']); // ticket 7
        $this->assertSame('no_response', $rows[7]['response_status']); // ticket 8
    }

    /**
     * Agent workload: tickets and responses per agent.
     */
    public function testAgentWorkload(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.assigned_agent,
                    COUNT(DISTINCT t.id) AS ticket_count,
                    COUNT(r.id) AS response_count
             FROM mi_hd_tickets t
             LEFT JOIN mi_hd_responses r ON r.ticket_id = t.id
             WHERE t.assigned_agent IS NOT NULL
             GROUP BY t.assigned_agent
             ORDER BY t.assigned_agent"
        );

        $this->assertCount(3, $rows);

        // agent_a: tickets 1,2,6 => 3 tickets; responses 1,2,3,4,9,10 => 6 responses
        $this->assertSame('agent_a', $rows[0]['assigned_agent']);
        $this->assertEquals(3, (int) $rows[0]['ticket_count']);
        $this->assertEquals(6, (int) $rows[0]['response_count']);

        // agent_b: tickets 3,4 => 2 tickets; responses 5,6 => 2 responses
        $this->assertSame('agent_b', $rows[1]['assigned_agent']);
        $this->assertEquals(2, (int) $rows[1]['ticket_count']);
        $this->assertEquals(2, (int) $rows[1]['response_count']);

        // agent_c: tickets 5,7 => 2 tickets; responses 7,8 => 2 responses
        $this->assertSame('agent_c', $rows[2]['assigned_agent']);
        $this->assertEquals(2, (int) $rows[2]['ticket_count']);
        $this->assertEquals(2, (int) $rows[2]['response_count']);
    }

    /**
     * Cross-tab: tickets by priority and status using conditional aggregation.
     */
    public function testTicketsByPriorityAndStatus(): void
    {
        $rows = $this->ztdQuery(
            "SELECT priority,
                    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_count,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress_count,
                    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_count,
                    COUNT(*) AS total
             FROM mi_hd_tickets
             GROUP BY priority
             ORDER BY CASE priority
                 WHEN 'critical' THEN 1
                 WHEN 'high' THEN 2
                 WHEN 'medium' THEN 3
                 WHEN 'low' THEN 4
             END"
        );

        $this->assertCount(4, $rows);

        // critical: 1 open, 1 in_progress, 1 resolved, total 3
        $this->assertSame('critical', $rows[0]['priority']);
        $this->assertEquals(1, (int) $rows[0]['open_count']);
        $this->assertEquals(1, (int) $rows[0]['in_progress_count']);
        $this->assertEquals(1, (int) $rows[0]['resolved_count']);
        $this->assertEquals(3, (int) $rows[0]['total']);

        // high: 1 open, 0 in_progress, 1 resolved, total 2
        $this->assertSame('high', $rows[1]['priority']);
        $this->assertEquals(1, (int) $rows[1]['open_count']);
        $this->assertEquals(0, (int) $rows[1]['in_progress_count']);
        $this->assertEquals(1, (int) $rows[1]['resolved_count']);
        $this->assertEquals(2, (int) $rows[1]['total']);

        // medium: 0 open, 0 in_progress, 1 resolved, total 1
        $this->assertSame('medium', $rows[2]['priority']);
        $this->assertEquals(0, (int) $rows[2]['open_count']);
        $this->assertEquals(0, (int) $rows[2]['in_progress_count']);
        $this->assertEquals(1, (int) $rows[2]['resolved_count']);
        $this->assertEquals(1, (int) $rows[2]['total']);

        // low: 1 open, 0 in_progress, 1 resolved, total 2
        $this->assertSame('low', $rows[3]['priority']);
        $this->assertEquals(1, (int) $rows[3]['open_count']);
        $this->assertEquals(0, (int) $rows[3]['in_progress_count']);
        $this->assertEquals(1, (int) $rows[3]['resolved_count']);
        $this->assertEquals(2, (int) $rows[3]['total']);
    }

    /**
     * Find tickets with no external responses using NOT EXISTS.
     */
    public function testUnrespondedTickets(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.subject, t.priority, t.status
             FROM mi_hd_tickets t
             WHERE NOT EXISTS (
                 SELECT 1 FROM mi_hd_responses r
                 WHERE r.ticket_id = t.id AND r.is_internal = 0
             )
             ORDER BY t.id"
        );

        $this->assertCount(3, $rows);

        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertSame('Feature request', $rows[0]['subject']);

        $this->assertEquals(7, (int) $rows[1]['id']);
        $this->assertSame('API timeout', $rows[1]['subject']);

        $this->assertEquals(8, (int) $rows[2]['id']);
        $this->assertSame('Billing question', $rows[2]['subject']);
    }

    /**
     * Count only external (non-internal) responses per ticket.
     */
    public function testResponseCountExcludingInternal(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.subject,
                    COUNT(r.id) AS external_responses
             FROM mi_hd_tickets t
             LEFT JOIN mi_hd_responses r ON r.ticket_id = t.id AND r.is_internal = 0
             GROUP BY t.id, t.subject
             ORDER BY t.id"
        );

        $this->assertCount(8, $rows);

        $this->assertEquals(2, (int) $rows[0]['external_responses']); // ticket 1
        $this->assertEquals(1, (int) $rows[1]['external_responses']); // ticket 2 (internal note excluded)
        $this->assertEquals(0, (int) $rows[2]['external_responses']); // ticket 3
        $this->assertEquals(2, (int) $rows[3]['external_responses']); // ticket 4
        $this->assertEquals(2, (int) $rows[4]['external_responses']); // ticket 5
        $this->assertEquals(2, (int) $rows[5]['external_responses']); // ticket 6
        $this->assertEquals(0, (int) $rows[6]['external_responses']); // ticket 7
        $this->assertEquals(0, (int) $rows[7]['external_responses']); // ticket 8
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_hd_tickets VALUES (9, 'New issue', 'low', 'open', '2025-10-05 10:00', 'agent_a')");
        $this->mysqli->query("UPDATE mi_hd_tickets SET status = 'closed' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_hd_tickets");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mi_hd_tickets WHERE id = 1");
        $this->assertSame('closed', $rows[0]['status']);

        // Physical tables untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_hd_tickets');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
