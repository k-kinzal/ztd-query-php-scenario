<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an employee skill matrix scenario through ZTD shadow store (MySQL PDO).
 * Skill gap analysis, team qualification checks, and training needs assessment.
 * SQL patterns exercised: HAVING COUNT = (SELECT COUNT) for fully-qualified matching,
 * NOT EXISTS for skill gap detection, MIN across related rows for minimum competency,
 * multiple JOINs (4 tables), GROUP BY with HAVING on aggregated expression,
 * SUM CASE for cross-tab skill level distribution, prepared statement for skill search,
 * physical isolation check.
 * @spec SPEC-10.2.153
 */
class MysqlSkillMatrixTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_sm_employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                department VARCHAR(50)
            )',
            'CREATE TABLE mp_sm_skills (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                category VARCHAR(20)
            )',
            'CREATE TABLE mp_sm_employee_skills (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT,
                skill_id INT,
                level INT
            )',
            'CREATE TABLE mp_sm_project_requirements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                project_name VARCHAR(50),
                skill_id INT,
                min_level INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_sm_project_requirements', 'mp_sm_employee_skills', 'mp_sm_skills', 'mp_sm_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Skills (6)
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (1, 'PHP', 'language')");
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (2, 'JavaScript', 'language')");
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (3, 'React', 'framework')");
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (4, 'Laravel', 'framework')");
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (5, 'Docker', 'tool')");
        $this->pdo->exec("INSERT INTO mp_sm_skills VALUES (6, 'Git', 'tool')");

        // Employees (5)
        $this->pdo->exec("INSERT INTO mp_sm_employees VALUES (1, 'Alice', 'engineering')");
        $this->pdo->exec("INSERT INTO mp_sm_employees VALUES (2, 'Bob', 'engineering')");
        $this->pdo->exec("INSERT INTO mp_sm_employees VALUES (3, 'Carol', 'engineering')");
        $this->pdo->exec("INSERT INTO mp_sm_employees VALUES (4, 'Dave', 'design')");
        $this->pdo->exec("INSERT INTO mp_sm_employees VALUES (5, 'Eve', 'engineering')");

        // Employee skills
        // Alice: PHP=5, JavaScript=4, React=3, Laravel=5, Docker=4, Git=5 (full-stack senior)
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (1, 1, 1, 5)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (2, 1, 2, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (3, 1, 3, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (4, 1, 4, 5)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (5, 1, 5, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (6, 1, 6, 5)");
        // Bob: PHP=4, JavaScript=3, Laravel=4, Git=3 (backend mid)
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (7, 2, 1, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (8, 2, 2, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (9, 2, 4, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (10, 2, 6, 3)");
        // Carol: JavaScript=5, React=5, Git=4 (frontend senior)
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (11, 3, 2, 5)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (12, 3, 3, 5)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (13, 3, 6, 4)");
        // Dave: JavaScript=2, React=2, Git=3 (junior designer)
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (14, 4, 2, 2)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (15, 4, 3, 2)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (16, 4, 6, 3)");
        // Eve: PHP=3, JavaScript=3, React=2, Laravel=2, Docker=3, Git=4 (full-stack junior)
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (17, 5, 1, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (18, 5, 2, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (19, 5, 3, 2)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (20, 5, 4, 2)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (21, 5, 5, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_employee_skills VALUES (22, 5, 6, 4)");

        // Project requirements
        // Alpha: PHP>=4, Laravel>=3, Docker>=3
        $this->pdo->exec("INSERT INTO mp_sm_project_requirements VALUES (1, 'Alpha', 1, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_project_requirements VALUES (2, 'Alpha', 4, 3)");
        $this->pdo->exec("INSERT INTO mp_sm_project_requirements VALUES (3, 'Alpha', 5, 3)");
        // Beta: JavaScript>=4, React>=4
        $this->pdo->exec("INSERT INTO mp_sm_project_requirements VALUES (4, 'Beta', 2, 4)");
        $this->pdo->exec("INSERT INTO mp_sm_project_requirements VALUES (5, 'Beta', 3, 4)");
    }

    /**
     * 3-table JOIN (employees, employee_skills, skills), GROUP BY employee,
     * COUNT skills, ROUND(AVG(level),1), MIN level per employee.
     * Order by avg DESC: Carol(4.7), Alice(4.3), Bob(3.5), Eve(2.8), Dave(2.3).
     */
    public function testEmployeeSkillSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name,
                    COUNT(es.id) AS skill_count,
                    ROUND(AVG(es.level), 1) AS avg_level,
                    MIN(es.level) AS min_level
             FROM mp_sm_employees e
             JOIN mp_sm_employee_skills es ON es.employee_id = e.id
             GROUP BY e.id, e.name
             ORDER BY avg_level DESC, e.name ASC"
        );

        $this->assertCount(5, $rows);

        // Carol: 3 skills, avg=4.7, min=4
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['skill_count']);
        $this->assertEquals(4.7, (float) $rows[0]['avg_level']);
        $this->assertEquals(4, (int) $rows[0]['min_level']);

        // Alice: 6 skills, avg=4.3, min=3
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertEquals(6, (int) $rows[1]['skill_count']);
        $this->assertEquals(4.3, (float) $rows[1]['avg_level']);
        $this->assertEquals(3, (int) $rows[1]['min_level']);

        // Bob: 4 skills, avg=3.5, min=3
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertEquals(4, (int) $rows[2]['skill_count']);
        $this->assertEquals(3.5, (float) $rows[2]['avg_level']);
        $this->assertEquals(3, (int) $rows[2]['min_level']);

        // Eve: 6 skills, avg=2.8, min=2
        $this->assertSame('Eve', $rows[3]['name']);
        $this->assertEquals(6, (int) $rows[3]['skill_count']);
        $this->assertEquals(2.8, (float) $rows[3]['avg_level']);
        $this->assertEquals(2, (int) $rows[3]['min_level']);

        // Dave: 3 skills, avg=2.3, min=2
        $this->assertSame('Dave', $rows[4]['name']);
        $this->assertEquals(3, (int) $rows[4]['skill_count']);
        $this->assertEquals(2.3, (float) $rows[4]['avg_level']);
        $this->assertEquals(2, (int) $rows[4]['min_level']);
    }

    /**
     * HAVING COUNT = (SELECT COUNT) for fully-qualified matching.
     * Employees who meet ALL requirements for project "Alpha" (PHP>=4, Laravel>=3, Docker>=3).
     * Only Alice qualifies (PHP=5, Laravel=5, Docker=4).
     */
    public function testFullyQualifiedForProject(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name
             FROM mp_sm_employees e
             JOIN mp_sm_employee_skills es ON es.employee_id = e.id
             JOIN mp_sm_project_requirements pr ON pr.skill_id = es.skill_id AND pr.project_name = 'Alpha'
             WHERE es.level >= pr.min_level
             GROUP BY e.id, e.name
             HAVING COUNT(DISTINCT pr.skill_id) = (SELECT COUNT(*) FROM mp_sm_project_requirements WHERE project_name = 'Alpha')"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * NOT EXISTS / LEFT JOIN for skill gap detection.
     * Find skills Bob is missing or underqualified for on project "Alpha".
     * Bob has PHP=4>=4, Laravel=4>=3, but no Docker at all.
     * Gap: Docker (min_level=3, current_level=0).
     */
    public function testSkillGapForProject(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name AS skill_name, pr.min_level,
                    COALESCE(es.level, 0) AS current_level
             FROM mp_sm_project_requirements pr
             JOIN mp_sm_skills s ON s.id = pr.skill_id
             LEFT JOIN mp_sm_employee_skills es ON es.skill_id = pr.skill_id AND es.employee_id = 2
             WHERE pr.project_name = 'Alpha'
               AND (es.level IS NULL OR es.level < pr.min_level)
             ORDER BY s.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Docker', $rows[0]['skill_name']);
        $this->assertEquals(3, (int) $rows[0]['min_level']);
        $this->assertEquals(0, (int) $rows[0]['current_level']);
    }

    /**
     * SUM CASE for cross-tab skill level distribution.
     * Categories: level 1-2 = beginner, 3-4 = intermediate, 5 = expert.
     * Verify Docker, Git, and React rows.
     */
    public function testSkillLevelDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name AS skill_name,
                    SUM(CASE WHEN es.level <= 2 THEN 1 ELSE 0 END) AS beginners,
                    SUM(CASE WHEN es.level BETWEEN 3 AND 4 THEN 1 ELSE 0 END) AS intermediate,
                    SUM(CASE WHEN es.level = 5 THEN 1 ELSE 0 END) AS experts,
                    COUNT(*) AS total_holders
             FROM mp_sm_skills s
             JOIN mp_sm_employee_skills es ON es.skill_id = s.id
             GROUP BY s.id, s.name
             ORDER BY s.name"
        );

        $this->assertCount(6, $rows);

        // Docker: 0 beginners, 2 intermediate (Alice=4, Eve=3), 0 experts, total=2
        $this->assertSame('Docker', $rows[0]['skill_name']);
        $this->assertEquals(0, (int) $rows[0]['beginners']);
        $this->assertEquals(2, (int) $rows[0]['intermediate']);
        $this->assertEquals(0, (int) $rows[0]['experts']);
        $this->assertEquals(2, (int) $rows[0]['total_holders']);

        // Git: 0 beginners, 4 intermediate (Bob=3, Carol=4, Dave=3, Eve=4), 1 expert (Alice=5), total=5
        $this->assertSame('Git', $rows[1]['skill_name']);
        $this->assertEquals(0, (int) $rows[1]['beginners']);
        $this->assertEquals(4, (int) $rows[1]['intermediate']);
        $this->assertEquals(1, (int) $rows[1]['experts']);
        $this->assertEquals(5, (int) $rows[1]['total_holders']);

        // React: 2 beginners (Dave=2, Eve=2), 1 intermediate (Alice=3), 1 expert (Carol=5), total=4
        $this->assertSame('React', $rows[5]['skill_name']);
        $this->assertEquals(2, (int) $rows[5]['beginners']);
        $this->assertEquals(1, (int) $rows[5]['intermediate']);
        $this->assertEquals(1, (int) $rows[5]['experts']);
        $this->assertEquals(4, (int) $rows[5]['total_holders']);
    }

    /**
     * MIN(level) per employee, HAVING MIN >= 3 means "no weak spots".
     * Alice min=3, Bob min=3, Carol min=4 qualify. Dave min=2, Eve min=2 do not.
     */
    public function testTeamMinimumCompetency(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, MIN(es.level) AS min_level
             FROM mp_sm_employees e
             JOIN mp_sm_employee_skills es ON es.employee_id = e.id
             GROUP BY e.id, e.name
             HAVING MIN(es.level) >= 3
             ORDER BY e.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['min_level']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['min_level']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(4, (int) $rows[2]['min_level']);
    }

    /**
     * Prepared statement: find all skills for a given employee, JOIN skills table,
     * ORDER BY level DESC, name ASC. Test with employee_id=1 (Alice).
     * Returns 6 rows: Git=5, Laravel=5, PHP=5, Docker=4, JavaScript=4, React=3.
     */
    public function testPreparedEmployeeSkills(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.name AS skill_name, es.level
             FROM mp_sm_employee_skills es
             JOIN mp_sm_skills s ON s.id = es.skill_id
             WHERE es.employee_id = ?
             ORDER BY es.level DESC, s.name ASC",
            [1]
        );

        $this->assertCount(6, $rows);

        // Level 5: Git, Laravel, PHP (alphabetical)
        $this->assertSame('Git', $rows[0]['skill_name']);
        $this->assertEquals(5, (int) $rows[0]['level']);
        $this->assertSame('Laravel', $rows[1]['skill_name']);
        $this->assertEquals(5, (int) $rows[1]['level']);
        $this->assertSame('PHP', $rows[2]['skill_name']);
        $this->assertEquals(5, (int) $rows[2]['level']);

        // Level 4: Docker, JavaScript (alphabetical)
        $this->assertSame('Docker', $rows[3]['skill_name']);
        $this->assertEquals(4, (int) $rows[3]['level']);
        $this->assertSame('JavaScript', $rows[4]['skill_name']);
        $this->assertEquals(4, (int) $rows[4]['level']);

        // Level 3: React
        $this->assertSame('React', $rows[5]['skill_name']);
        $this->assertEquals(3, (int) $rows[5]['level']);
    }

    /**
     * Physical isolation: insert a new employee_skill through ZTD,
     * verify shadow count incremented, then disableZtd and verify physical count is 0.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new employee_skill through ZTD
        $this->pdo->exec(
            "INSERT INTO mp_sm_employee_skills VALUES (23, 2, 5, 2)"
        );

        // ZTD sees the new record (22 original + 1 new = 23)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_sm_employee_skills");
        $this->assertEquals(23, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_sm_employee_skills')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
