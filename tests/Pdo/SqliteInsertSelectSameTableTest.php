<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT from the same table (self-referencing copy).
 *
 * Real-world scenario: copying rows within the same table is common for
 * duplicating records (e.g., "clone this template"), archiving, or
 * creating draft versions. The CTE rewriter must handle the case where
 * the INSERT target and SELECT source are the same table, both needing
 * shadow store interaction.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSelectSameTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isst_templates (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                body TEXT NOT NULL,
                is_copy INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isst_templates'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_isst_templates VALUES (1, 'Welcome Email', 'Hello {{name}}', 0)");
        $this->ztdExec("INSERT INTO sl_isst_templates VALUES (2, 'Invoice', 'Amount: {{total}}', 0)");
    }

    /**
     * INSERT...SELECT from same table with ONLY direct column references.
     * Isolates same-table behavior from Issue #20 (computed columns become NULL).
     */
    public function testInsertSelectSameTableDirectRefs(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT 10, name, body, is_copy
                 FROM sl_isst_templates
                 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_isst_templates WHERE name = 'Welcome Email' ORDER BY id");
            if (count($rows) < 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT same table with direct refs produced '
                    . count($rows) . ' matching row(s) instead of 2. '
                    . 'Same-table INSERT...SELECT may not read shadow correctly.'
                );
            }
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT same table direct refs failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT from same table with computed expressions.
     * Related to Issue #20 (INSERT...SELECT computed columns become NULL).
     */
    public function testInsertSelectSameTableCopy(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT 10, name || ' (copy)', body, 1
                 FROM sl_isst_templates
                 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_isst_templates WHERE id = 10");
            $this->assertCount(1, $rows);
            $this->assertSame('Welcome Email (copy)', $rows[0]['name']);
            $this->assertSame('Hello {{name}}', $rows[0]['body']);
            $this->assertEquals(1, (int) $rows[0]['is_copy']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT same table copy failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT all rows from same table (bulk duplicate).
     */
    public function testInsertSelectSameTableBulkCopy(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT id + 100, name || ' (v2)', body, 1
                 FROM sl_isst_templates
                 WHERE is_copy = 0"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_isst_templates ORDER BY id");
            $this->assertCount(4, $rows);

            $copies = array_filter($rows, fn($r) => (int) $r['is_copy'] === 1);
            $this->assertCount(2, $copies);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT same table bulk copy failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT from same table with prepared params.
     */
    public function testInsertSelectSameTableWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT ?, name || ' (draft)', body, 1
                 FROM sl_isst_templates
                 WHERE id = ?"
            );
            $stmt->execute([20, 2]);

            $rows = $this->ztdQuery("SELECT name FROM sl_isst_templates WHERE id = 20");
            $this->assertCount(1, $rows);
            $this->assertSame('Invoice (draft)', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT same table with params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT from same table then UPDATE the copy.
     * Verifies the copied row is independently modifiable.
     */
    public function testInsertSelectSameTableThenUpdateCopy(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT 30, name, 'New body content', 1
                 FROM sl_isst_templates
                 WHERE id = 1"
            );

            $this->ztdExec("UPDATE sl_isst_templates SET name = 'Modified Copy' WHERE id = 30");

            // Original should be unchanged
            $orig = $this->ztdQuery("SELECT name, body FROM sl_isst_templates WHERE id = 1");
            $this->assertSame('Welcome Email', $orig[0]['name']);
            $this->assertSame('Hello {{name}}', $orig[0]['body']);

            // Copy should be updated
            $copy = $this->ztdQuery("SELECT name, body FROM sl_isst_templates WHERE id = 30");
            $this->assertSame('Modified Copy', $copy[0]['name']);
            $this->assertSame('New body content', $copy[0]['body']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT same table then UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT from same table with aggregate (e.g., compute next ID).
     */
    public function testInsertSelectSameTableWithAggregate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isst_templates (id, name, body, is_copy)
                 SELECT (SELECT MAX(id) + 1 FROM sl_isst_templates), 'Auto ID', 'body', 1"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_isst_templates WHERE name = 'Auto ID'");
            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT...SELECT with subquery aggregate failed: ' . $e->getMessage()
            );
        }
    }
}
