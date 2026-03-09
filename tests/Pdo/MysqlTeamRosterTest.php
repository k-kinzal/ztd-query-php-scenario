<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests team roster with string aggregation via GROUP_CONCAT in multi-table JOINs (MySQL PDO).
 * SQL patterns exercised: GROUP_CONCAT with ORDER BY and SEPARATOR in JOIN + GROUP BY,
 * GROUP_CONCAT after INSERT/DELETE, HAVING COUNT on grouped aggregate,
 * prepared statement returning GROUP_CONCAT.
 * @spec SPEC-10.2.172
 */
class MysqlTeamRosterTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_tr_team (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_tr_member (
                id INT PRIMARY KEY,
                team_id INT NOT NULL,
                name VARCHAR(100) NOT NULL,
                role VARCHAR(50) NOT NULL,
                active TINYINT NOT NULL DEFAULT 1
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_tr_member', 'mp_tr_team'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_tr_team VALUES (1, 'Backend', 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_tr_team VALUES (2, 'Frontend', 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_tr_team VALUES (3, 'Design', 'Product')");
        $this->pdo->exec("INSERT INTO mp_tr_team VALUES (4, 'QA', 'Engineering')");

        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (1, 1, 'Alice', 'lead', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (2, 1, 'Bob', 'developer', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (3, 1, 'Carol', 'developer', 0)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (4, 2, 'Dave', 'lead', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (5, 2, 'Eve', 'developer', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (6, 3, 'Frank', 'designer', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (7, 3, 'Grace', 'designer', 1)");
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (8, 3, 'Heidi', 'lead', 1)");
    }

    /**
     * GROUP_CONCAT with ORDER BY and SEPARATOR in multi-table JOIN.
     */
    public function testGroupConcatMemberNamesPerTeam(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        $this->assertCount(3, $rows); // QA has no members, excluded by INNER JOIN

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
     * GROUP_CONCAT with WHERE filter — only active members.
     */
    public function testGroupConcatActiveOnly(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS active_members,
                    COUNT(m.id) AS active_count
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
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
     * LEFT JOIN GROUP_CONCAT — teams with zero members get NULL aggregate.
     */
    public function testLeftJoinGroupConcatIncludesEmptyTeams(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM mp_tr_team t
             LEFT JOIN mp_tr_member m ON m.team_id = t.id
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
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
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
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
             WHERE t.id = 4
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('QA', $rows[0]['team']);
        $this->assertSame('Ivan', $rows[0]['members']);
    }

    /**
     * GROUP_CONCAT after DELETE — removed member disappears from list.
     */
    public function testGroupConcatAfterDelete(): void
    {
        $this->ztdExec("DELETE FROM mp_tr_member WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members,
                    COUNT(m.id) AS member_count
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
             WHERE t.id = 1
             GROUP BY t.id, t.name"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['member_count']);
        $this->assertSame('Alice, Carol', $rows[0]['members']);
    }

    /**
     * GROUP_CONCAT grouped by role across all teams.
     */
    public function testGroupConcatByRole(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.role,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members,
                    COUNT(m.id) AS cnt
             FROM mp_tr_member m
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
     * Prepared statement: filter teams by department, return GROUP_CONCAT.
     */
    public function testPreparedGroupConcatByDepartment(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT t.name AS team,
                    GROUP_CONCAT(m.name ORDER BY m.name SEPARATOR ', ') AS members
             FROM mp_tr_team t
             JOIN mp_tr_member m ON m.team_id = t.id
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
        $this->pdo->exec("INSERT INTO mp_tr_member VALUES (9, 4, 'Ivan', 'tester', 1)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_tr_member");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_tr_member")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
