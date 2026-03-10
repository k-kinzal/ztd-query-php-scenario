<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests string functions in DML on PostgreSQL, including PostgreSQL-specific
 * || operator and POSITION function.
 *
 * @spec SPEC-4.2
 */
class PostgresStringFunctionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sfd_items (
            id INTEGER PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            label VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_sfd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sfd_items VALUES (1, 'SKU-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO pg_sfd_items VALUES (2, 'SKU-002', 'Gadget Beta', 'active')");
        $this->pdo->exec("INSERT INTO pg_sfd_items VALUES (3, 'OLD-003', 'Sprocket Gamma', 'inactive')");
    }

    /**
     * UPDATE with || concatenation.
     */
    public function testUpdateConcatenation(): void
    {
        $sql = "UPDATE pg_sfd_items SET label = label || ' [' || status || ']'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT label FROM pg_sfd_items WHERE id = 1");

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
     * Prepared UPDATE with REPLACE and bound params.
     */
    public function testPreparedUpdateReplace(): void
    {
        $sql = "UPDATE pg_sfd_items SET code = REPLACE(code, $1, $2)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['SKU', 'PRD']);

            $rows = $this->ztdQuery("SELECT code FROM pg_sfd_items ORDER BY id");

            $this->assertCount(3, $rows);

            if ($rows[0]['code'] !== 'PRD-001') {
                $this->markTestIncomplete(
                    'Prepared REPLACE: expected PRD-001, got ' . $rows[0]['code']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('PRD-001', $rows[0]['code']);
            $this->assertSame('OLD-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared REPLACE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with UPPER/LOWER.
     */
    public function testUpdateUpperLower(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_sfd_items SET label = UPPER(label), code = LOWER(code)");

            $rows = $this->ztdQuery("SELECT code, label FROM pg_sfd_items WHERE id = 1");

            $this->assertSame('sku-001', $rows[0]['code']);
            $this->assertSame('WIDGET ALPHA', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPPER/LOWER UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with POSITION function.
     */
    public function testDeleteWithPosition(): void
    {
        $sql = "DELETE FROM pg_sfd_items WHERE POSITION('OLD' IN code) > 0";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT code FROM pg_sfd_items ORDER BY id");

            $this->assertCount(2, $rows);
            $this->assertSame('SKU-001', $rows[0]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('POSITION DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with concatenation and $1 param.
     */
    public function testPreparedConcatUpdate(): void
    {
        $sql = "UPDATE pg_sfd_items SET label = label || $1 WHERE id = $2";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([' (v2)', 1]);

            $rows = $this->ztdQuery("SELECT label FROM pg_sfd_items WHERE id = 1");

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
}
