<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests team roster with string aggregation via GROUP_CONCAT in multi-table JOINs.
 * SQL patterns exercised: GROUP_CONCAT with separator in JOIN + GROUP BY,
 * GROUP_CONCAT after INSERT/DELETE (shadow mutation visibility in aggregate),
 * HAVING COUNT on grouped aggregate, prepared statement returning GROUP_CONCAT.
 * @spec SPEC-10.2.172
 */
class SqliteTeamRosterTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tr_team (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL
            )',
            'CREATE TABLE sl_tr_member (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tr_member', 'sl_tr_team'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_tr_team VALUES (1, 'Backend', 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_tr_team VALUES (2, 'Frontend', 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_tr_team VALUES (3, 'Design', 'Product')");
        $this->pdo->exec("INSERT INTO sl_tr_team VALUES (4, 'QA', 'Engineering')");

        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (1, 1, 'Alice', 'lead', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (2, 1, 'Bob', 'developer', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (3, 1, 'Carol', 'developer', 0)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (4, 2, 'Dave', 'lead', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (5, 2, 'Eve', 'developer', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (6, 3, 'Frank', 'designer', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (7, 3, 'Grace', 'designer', 1)");
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (8, 3, 'Heidi', 'lead', 1)");
    }

    /**
     * GROUP_CONCAT of member names per team via JOIN + GROUP BY.
     */
    public function testGroupConcatMemberNamesPerTeam(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        // INNER JOIN excludes QA (no members)
        $this->assertCount(3, $rows);

        // Backend: Alice, Bob, Carol (3 members including inactive)
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertEquals(3, (int) $rows[0]['member_count']);
        $members = $rows[0]['members'];
        $this->assertStringContainsString('Alice', $members);
        $this->assertStringContainsString('Bob', $members);
        $this->assertStringContainsString('Carol', $members);

        // Design: Frank, Grace, Heidi
        $this->assertSame('Design', $rows[1]['team']);
        $this->assertEquals(3, (int) $rows[1]['member_count']);

        // Frontend: Dave, Eve
        $this->assertSame('Frontend', $rows[2]['team']);
        $this->assertEquals(2, (int) $rows[2]['member_count']);
    }

    /**
     * GROUP_CONCAT with WHERE filter — only active members.
     */
    public function testGroupConcatActiveOnly(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS active_members,
                    COUNT(m.id) AS active_count
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             WHERE m.active = 1
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        // QA has no members at all, Backend has 2 active (Carol is inactive)
        $backendRow = null;
        foreach ($rows as $row) {
            if ($row['team'] === 'Backend') {
                $backendRow = $row;
                break;
            }
        }
        $this->assertNotNull($backendRow);
        $this->assertEquals(2, (int) $backendRow['active_count']);
        $this->assertStringContainsString('Alice', $backendRow['active_members']);
        $this->assertStringContainsString('Bob', $backendRow['active_members']);
        $this->assertStringNotContainsString('Carol', $backendRow['active_members']);
    }

    /**
     * LEFT JOIN GROUP_CONCAT — teams with zero members get NULL aggregate.
     */
    public function testLeftJoinGroupConcatIncludesEmptyTeams(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM sl_tr_team t
             LEFT JOIN sl_tr_member m ON m.team_id = t.id
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
    public function testHavingCountOnGroupConcat(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             HAVING COUNT(m.id) > 2
             ORDER BY t.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Design', $rows[1]['team']);
    }

    /**
     * GROUP_CONCAT after INSERT — new member appears in concatenated list.
     */
    public function testGroupConcatAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             WHERE t.id = 4
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('QA', $rows[0]['team']);
        $this->assertStringContainsString('Ivan', $rows[0]['members']);
    }

    /**
     * GROUP_CONCAT after DELETE — removed member disappears from concatenated list.
     */
    public function testGroupConcatAfterDelete(): void
    {
        $this->ztdExec("DELETE FROM sl_tr_member WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             WHERE t.id = 1
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['member_count']);
        $this->assertStringNotContainsString('Bob', $rows[0]['members']);
        $this->assertStringContainsString('Alice', $rows[0]['members']);
    }

    /**
     * GROUP_CONCAT with GROUP BY on role — aggregate by role across teams.
     */
    public function testGroupConcatByRole(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.role,
                    GROUP_CONCAT(m.name, ', ') AS members,
                    COUNT(m.id) AS cnt
             FROM sl_tr_member m
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
        $this->assertStringContainsString('Frank', $designerRow['members']);
        $this->assertStringContainsString('Grace', $designerRow['members']);
    }

    /**
     * Prepared statement: filter teams by department, return GROUP_CONCAT.
     */
    public function testPreparedGroupConcatByDepartment(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name, ', ') AS members
             FROM sl_tr_team t
             JOIN sl_tr_member m ON m.team_id = t.id
             WHERE t.department = ? AND m.active = 1
             GROUP BY t.id, t.name
             ORDER BY t.name",
            ['Engineering']
        );

        // Engineering teams with active members: Backend (Alice, Bob), Frontend (Dave, Eve)
        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Frontend', $rows[1]['team']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_tr_member");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_tr_member")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
