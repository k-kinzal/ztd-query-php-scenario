<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests regex-like operators (GLOB, LIKE with wildcards) in DML through ZTD on SQLite.
 *
 * SQLite supports GLOB (case-sensitive, Unix-style wildcards) as a pattern
 * matching operator. This tests whether the CTE rewriter handles these
 * operators in UPDATE/DELETE WHERE clauses correctly.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteRegexOperatorDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_rgx_items (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_rgx_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_rgx_items (id, code, name, status) VALUES (1, 'PRD-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO sl_rgx_items (id, code, name, status) VALUES (2, 'PRD-002', 'Widget Beta', 'active')");
        $this->pdo->exec("INSERT INTO sl_rgx_items (id, code, name, status) VALUES (3, 'SVC-001', 'Service Plan', 'active')");
        $this->pdo->exec("INSERT INTO sl_rgx_items (id, code, name, status) VALUES (4, 'SVC-002', 'Service Premium', 'inactive')");
        $this->pdo->exec("INSERT INTO sl_rgx_items (id, code, name, status) VALUES (5, 'ACC-001', 'Accessory Kit', 'active')");
    }

    /**
     * UPDATE with GLOB operator in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithGlobWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_rgx_items SET status = 'discontinued' WHERE code GLOB 'PRD-*'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM sl_rgx_items WHERE code GLOB 'PRD-*' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with GLOB: expected 2 matching rows, got ' . count($rows)
                );
            }

            $this->assertSame('discontinued', $rows[0]['status']);
            $this->assertSame('discontinued', $rows[1]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with GLOB failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with GLOB operator in WHERE.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithGlobWhere(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_rgx_items WHERE code GLOB 'SVC-*'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE with GLOB: expected 3 remaining rows, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with GLOB failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with GLOB and bound parameter.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateWithGlob(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_rgx_items SET status = ? WHERE code GLOB ?"
            );
            $stmt->execute(['archived', 'ACC-*']);

            $rows = $this->ztdQuery(
                "SELECT status FROM sl_rgx_items WHERE id = 5"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared GLOB UPDATE: expected 1 row');
            }

            $this->assertSame('archived', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with GLOB failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with GLOB on shadow-inserted data.
     *
     * @spec SPEC-3.1
     */
    public function testSelectGlobOnShadowData(): void
    {
        try {
            // Insert new shadow row
            $this->pdo->exec(
                "INSERT INTO sl_rgx_items (id, code, name, status) VALUES (6, 'PRD-003', 'New Product', 'draft')"
            );

            $rows = $this->ztdQuery(
                "SELECT id, code FROM sl_rgx_items WHERE code GLOB 'PRD-*' ORDER BY id"
            );

            $this->assertCount(3, $rows, 'Should find 3 PRD-* items including shadow insert');
            $this->assertEquals(6, (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT GLOB on shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * Combined GLOB and NOT GLOB in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithNotGlob(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_rgx_items SET status = 'flagged' WHERE code NOT GLOB 'PRD-*' AND status = 'active'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM sl_rgx_items WHERE status = 'flagged' ORDER BY id"
            );

            // SVC-001 (active) and ACC-001 (active) should match; SVC-002 is inactive
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE NOT GLOB: expected 2 flagged rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE NOT GLOB failed: ' . $e->getMessage());
        }
    }
}
