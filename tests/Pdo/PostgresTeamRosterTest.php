<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests team roster with string aggregation via STRING_AGG in multi-table JOINs (PostgreSQL PDO).
 * SQL patterns exercised: STRING_AGG with ORDER BY in JOIN + GROUP BY,
 * STRING_AGG after INSERT/DELETE, HAVING COUNT on grouped aggregate,
 * prepared statement returning STRING_AGG.
 * @spec SPEC-10.2.172
 */
class PostgresTeamRosterTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_tr_team (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE pg_tr_member (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL,
                name VARCHAR(100) NOT NULL,
                role VARCHAR(50) NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_tr_member', 'pg_tr_team'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_tr_team VALUES (1, 'Backend', 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_tr_team VALUES (2, 'Frontend', 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_tr_team VALUES (3, 'Design', 'Product')");
        $this->pdo->exec("INSERT INTO pg_tr_team VALUES (4, 'QA', 'Engineering')");

        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (1, 1, 'Alice', 'lead', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (2, 1, 'Bob', 'developer', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (3, 1, 'Carol', 'developer', 0)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (4, 2, 'Dave', 'lead', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (5, 2, 'Eve', 'developer', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (6, 3, 'Frank', 'designer', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (7, 3, 'Grace', 'designer', 1)");
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (8, 3, 'Heidi', 'lead', 1)");
    }

    /**
     * STRING_AGG of member names per team via JOIN + GROUP BY.
     */
    public function testStringAggMemberNamesPerTeam(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members,
                    COUNT(m.id) AS member_count
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertEquals(3, (int) $rows[0]['member_count']);
        $this->assertSame('Alice, Bob, Carol', $rows[0]['members']);

        $this->assertSame('Design', $rows[1]['team']);
        $this->assertEquals(3, (int) $rows[1]['member_count']);
        $this->assertSame('Frank, Grace, Heidi', $rows[1]['members']);

        $this->assertSame('Frontend', $rows[2]['team']);
        $this->assertEquals(2, (int) $rows[2]['member_count']);
        $this->assertSame('Dave, Eve', $rows[2]['members']);
    }

    /**
     * STRING_AGG with WHERE filter — only active members.
     */
    public function testStringAggActiveOnly(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS active_members,
                    COUNT(m.id) AS active_count
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             WHERE m.active = 1
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        $backendRow = null;
        foreach ($rows as $row) {
            if ($row['team'] === 'Backend') {
                $backendRow = $row;
                break;
            }
        }
        $this->assertNotNull($backendRow);
        $this->assertEquals(2, (int) $backendRow['active_count']);
        $this->assertSame('Alice, Bob', $backendRow['active_members']);
    }

    /**
     * LEFT JOIN STRING_AGG — teams with zero members get NULL aggregate.
     */
    public function testLeftJoinStringAggIncludesEmptyTeams(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members,
                    COUNT(m.id) AS member_count
             FROM pg_tr_team t
             LEFT JOIN pg_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        $this->assertCount(4, $rows);

        $qaRow = null;
        foreach ($rows as $row) {
            if ($row['team'] === 'QA') {
                $qaRow = $row;
                break;
            }
        }
        $this->assertNotNull($qaRow);
        $this->assertEquals(0, (int) $qaRow['member_count']);
        $this->assertNull($qaRow['members']);
    }

    /**
     * HAVING COUNT filter — only teams with > 2 members.
     */
    public function testHavingCountOnStringAgg(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members,
                    COUNT(m.id) AS member_count
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             HAVING COUNT(m.id) > 2
             ORDER BY t.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Design', $rows[1]['team']);
    }

    /**
     * STRING_AGG after INSERT — new member appears in concatenated list.
     */
    public function testStringAggAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             WHERE t.id = 4
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('QA', $rows[0]['team']);
        $this->assertSame('Ivan', $rows[0]['members']);
    }

    /**
     * STRING_AGG after DELETE — removed member disappears from list.
     */
    public function testStringAggAfterDelete(): void
    {
        $this->ztdExec("DELETE FROM pg_tr_member WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members,
                    COUNT(m.id) AS member_count
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             WHERE t.id = 1
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['member_count']);
        $this->assertSame('Alice, Carol', $rows[0]['members']);
    }

    /**
     * STRING_AGG grouped by role across all teams.
     */
    public function testStringAggByRole(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.role,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members,
                    COUNT(m.id) AS cnt
             FROM pg_tr_member m
             WHERE m.active = 1
             GROUP BY m.role
             ORDER BY m.role"
        );

        $this->assertGreaterThanOrEqual(3, count($rows));

        $designerRow = null;
        foreach ($rows as $row) {
            if ($row['role'] === 'designer') {
                $designerRow = $row;
                break;
            }
        }
        $this->assertNotNull($designerRow);
        $this->assertEquals(2, (int) $designerRow['cnt']);
        $this->assertSame('Frank, Grace', $designerRow['members']);
    }

    /**
     * Prepared statement: filter teams by department, return STRING_AGG.
     */
    public function testPreparedStringAggByDepartment(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT t.name AS team,
                    STRING_AGG(m.name, ', ' ORDER BY m.name) AS members
             FROM pg_tr_team t
             JOIN pg_tr_member m ON m.team_id = t.id
             WHERE t.department = ? AND m.active = 1
             GROUP BY t.id, t.name
             ORDER BY t.name",
            ['Engineering']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Alice, Bob', $rows[0]['members']);
        $this->assertSame('Frontend', $rows[1]['team']);
        $this->assertSame('Dave, Eve', $rows[1]['members']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_tr_member");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_tr_member")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
