<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multi-column EXCEPT and EXCEPT ALL on PostgreSQL through the CTE shadow store.
 *
 * Multi-column INTERSECT already passes; this verifies that the same holds for EXCEPT,
 * which uses comparable CTE rewriting logic.
 */
class PostgresMultiColumnExceptTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mce_employees (
                id SERIAL PRIMARY KEY,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
            'CREATE TABLE pg_mce_contractors (
                id SERIAL PRIMARY KEY,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mce_employees', 'pg_mce_contractors'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees: some overlap with contractors, some unique
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (1, 'engineering', 'python')");
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (2, 'engineering', 'java')");
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (3, 'engineering', 'go')");
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (4, 'marketing', 'seo')");
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (5, 'marketing', 'analytics')");
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (6, 'design', 'figma')");
        // Duplicate row for EXCEPT ALL testing: a second (engineering, python) pair
        $this->pdo->exec("INSERT INTO pg_mce_employees (id, department, skill) VALUES (7, 'engineering', 'python')");

        // Contractors: overlap on (engineering, python) and (marketing, seo)
        $this->pdo->exec("INSERT INTO pg_mce_contractors (id, department, skill) VALUES (1, 'engineering', 'python')");
        $this->pdo->exec("INSERT INTO pg_mce_contractors (id, department, skill) VALUES (2, 'marketing', 'seo')");
        $this->pdo->exec("INSERT INTO pg_mce_contractors (id, department, skill) VALUES (3, 'sales', 'crm')");
    }

    /**
     * Multi-column EXCEPT removes overlapping (department, skill) pairs.
     *
     * Employee-only pairs: (engineering, java), (engineering, go),
     *                      (marketing, analytics), (design, figma)
     * Overlapping pairs removed: (engineering, python), (marketing, seo)
     */
    public function testMultiColumnExcept(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM pg_mce_employees
             EXCEPT
             SELECT department, skill FROM pg_mce_contractors
             ORDER BY department, skill"
        );

        $expected = [
            ['department' => 'design',      'skill' => 'figma'],
            ['department' => 'engineering',  'skill' => 'go'],
            ['department' => 'engineering',  'skill' => 'java'],
            ['department' => 'marketing',    'skill' => 'analytics'],
        ];

        $this->assertCount(4, $rows, 'EXCEPT should return 4 employee-only (department, skill) pairs');
        $this->assertSame($expected, $rows);
    }

    /**
     * Multi-column EXCEPT ALL preserves duplicate counts.
     *
     * Employees have (engineering, python) x2. Contractors have it x1.
     * EXCEPT ALL should keep one copy of (engineering, python) plus all other unique pairs.
     *
     * Expected (unordered): (engineering, python), (engineering, java), (engineering, go),
     *                        (marketing, analytics), (design, figma)
     */
    public function testMultiColumnExceptAll(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM pg_mce_employees
             EXCEPT ALL
             SELECT department, skill FROM pg_mce_contractors
             ORDER BY department, skill"
        );

        $expected = [
            ['department' => 'design',      'skill' => 'figma'],
            ['department' => 'engineering',  'skill' => 'go'],
            ['department' => 'engineering',  'skill' => 'java'],
            ['department' => 'engineering',  'skill' => 'python'],
            ['department' => 'marketing',    'skill' => 'analytics'],
        ];

        $this->assertCount(5, $rows, 'EXCEPT ALL should return 5 rows (one copy of the duplicate pair survives)');
        $this->assertSame($expected, $rows);
    }

    /**
     * Physical isolation — shadow data does not leak to the physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Verify data is visible through ZTD
        $ztdRows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_mce_employees");
        $this->assertSame(7, (int) $ztdRows[0]['cnt']);

        // Bypass ZTD: physical table must be empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_mce_employees");
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $result[0]['cnt'], 'Physical table must have zero rows');

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_mce_contractors");
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $result[0]['cnt'], 'Physical table must have zero rows');
    }
}
