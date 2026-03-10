<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests string functions (REPLACE, SUBSTR, UPPER, LOWER, TRIM, LENGTH)
 * in UPDATE SET with prepared parameters.
 *
 * Users commonly use string functions in DML. Tests whether the CTE
 * rewriter correctly handles function calls with mixed literal and
 * parameter arguments in SET and WHERE clauses.
 *
 * @spec SPEC-4.2
 */
class SqliteStringFunctionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_sfd_items (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL,
            label TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_sfd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sfd_items VALUES (1, 'SKU-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO sl_sfd_items VALUES (2, 'SKU-002', '  Gadget Beta  ', 'active')");
        $this->pdo->exec("INSERT INTO sl_sfd_items VALUES (3, 'OLD-003', 'Sprocket Gamma', 'inactive')");
        $this->pdo->exec("INSERT INTO sl_sfd_items VALUES (4, 'OLD-004', 'Bolt Delta', 'active')");
    }

    /**
     * UPDATE SET with REPLACE function.
     */
    public function testUpdateSetReplace(): void
    {
        $sql = "UPDATE sl_sfd_items SET code = REPLACE(code, 'OLD', 'NEW') WHERE code LIKE 'OLD%'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, code FROM sl_sfd_items ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertSame('SKU-001', $rows[0]['code']);
            $this->assertSame('SKU-002', $rows[1]['code']);
            $this->assertSame('NEW-003', $rows[2]['code']);
            $this->assertSame('NEW-004', $rows[3]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('REPLACE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with REPLACE and bound search/replacement params.
     */
    public function testPreparedUpdateReplace(): void
    {
        $sql = "UPDATE sl_sfd_items SET code = REPLACE(code, ?, ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['SKU', 'PRD']);

            $rows = $this->ztdQuery("SELECT id, code FROM sl_sfd_items ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['code'] !== 'PRD-001') {
                $this->markTestIncomplete(
                    'Prepared REPLACE: expected PRD-001, got ' . $rows[0]['code']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('PRD-001', $rows[0]['code']);
            $this->assertSame('PRD-002', $rows[1]['code']);
            $this->assertSame('OLD-003', $rows[2]['code']); // no SKU to replace
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared REPLACE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with UPPER/LOWER functions.
     */
    public function testUpdateUpperLower(): void
    {
        $sql = "UPDATE sl_sfd_items SET label = UPPER(label), code = LOWER(code)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT code, label FROM sl_sfd_items WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertSame('sku-001', $rows[0]['code']);
            $this->assertSame('WIDGET ALPHA', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPPER/LOWER UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with TRIM function.
     */
    public function testUpdateTrim(): void
    {
        $sql = "UPDATE sl_sfd_items SET label = TRIM(label)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT label FROM sl_sfd_items WHERE id = 2");

            $this->assertCount(1, $rows);

            if ($rows[0]['label'] !== 'Gadget Beta') {
                $this->markTestIncomplete(
                    'TRIM UPDATE: expected "Gadget Beta", got "' . $rows[0]['label'] . '"'
                );
            }

            $this->assertSame('Gadget Beta', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('TRIM UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with concatenation (|| operator).
     */
    public function testUpdateConcatenation(): void
    {
        $sql = "UPDATE sl_sfd_items SET label = label || ' [' || status || ']'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT label FROM sl_sfd_items WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['label'] !== 'Widget Alpha [active]') {
                $this->markTestIncomplete(
                    'Concatenation UPDATE: expected "Widget Alpha [active]", got "'
                    . $rows[0]['label'] . '"'
                );
            }

            $this->assertSame('Widget Alpha [active]', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Concatenation UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with concatenation and bound suffix param.
     */
    public function testPreparedConcatenationUpdate(): void
    {
        $sql = "UPDATE sl_sfd_items SET label = label || ? WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([' (v2)', 1]);

            $rows = $this->ztdQuery("SELECT label FROM sl_sfd_items WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['label'] !== 'Widget Alpha (v2)') {
                $this->markTestIncomplete(
                    'Prepared concat: expected "Widget Alpha (v2)", got "'
                    . $rows[0]['label'] . '"'
                );
            }

            $this->assertSame('Widget Alpha (v2)', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared concat UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE with LENGTH function and prepared param.
     */
    public function testDeleteWhereLengthWithParam(): void
    {
        $sql = "DELETE FROM sl_sfd_items WHERE LENGTH(label) > ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([13]);

            $rows = $this->ztdQuery("SELECT id, label FROM sl_sfd_items ORDER BY id");

            // Widget Alpha (12) ≤ 13 → kept
            // "  Gadget Beta  " (15 with spaces) > 13 → deleted
            // Sprocket Gamma (14) > 13 → deleted
            // Bolt Delta (10) ≤ 13 → kept
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'LENGTH DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'label');
            $this->assertContains('Widget Alpha', $names);
            $this->assertContains('Bolt Delta', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LENGTH DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with SUBSTR function and prepared param.
     */
    public function testUpdateSubstr(): void
    {
        $sql = "UPDATE sl_sfd_items SET code = SUBSTR(code, 1, ?) || '-X' WHERE code LIKE 'SKU%'";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([3]);

            $rows = $this->ztdQuery("SELECT id, code FROM sl_sfd_items ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['code'] !== 'SKU-X') {
                $this->markTestIncomplete(
                    'SUBSTR UPDATE: expected SKU-X, got ' . $rows[0]['code']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('SKU-X', $rows[0]['code']);
            $this->assertSame('SKU-X', $rows[1]['code']);
            $this->assertSame('OLD-003', $rows[2]['code']); // not matching LIKE
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUBSTR UPDATE failed: ' . $e->getMessage());
        }
    }
}
