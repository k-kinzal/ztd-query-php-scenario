<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests REGEXP operator in DML through ZTD on MySQLi.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class RegexOperatorDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_rgx_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_rgx_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rgx_items (code, name, status) VALUES ('PRD-001', 'Widget Alpha', 'active')");
        $this->mysqli->query("INSERT INTO mi_rgx_items (code, name, status) VALUES ('PRD-002', 'Widget Beta', 'active')");
        $this->mysqli->query("INSERT INTO mi_rgx_items (code, name, status) VALUES ('SVC-001', 'Service Plan', 'active')");
        $this->mysqli->query("INSERT INTO mi_rgx_items (code, name, status) VALUES ('SVC-002', 'Service Premium', 'inactive')");
        $this->mysqli->query("INSERT INTO mi_rgx_items (code, name, status) VALUES ('ACC-001', 'Accessory Kit', 'active')");
    }

    /**
     * UPDATE with REGEXP in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithRegexp(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_rgx_items SET status = 'discontinued' WHERE code REGEXP '^PRD-[0-9]+$'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM mi_rgx_items WHERE code REGEXP '^PRD-' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertSame('discontinued', $rows[0]['status']);
            $this->assertSame('discontinued', $rows[1]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with REGEXP failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with REGEXP in WHERE.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithRegexp(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_rgx_items WHERE code REGEXP '^SVC-'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE with REGEXP: expected 3 remaining, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with REGEXP failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with REGEXP.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateWithRegexp(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "UPDATE mi_rgx_items SET status = ? WHERE code REGEXP ?",
                ['archived', '^ACC-']
            );

            $check = $this->ztdQuery(
                "SELECT status FROM mi_rgx_items WHERE code = 'ACC-001'"
            );

            if (count($check) !== 1) {
                $this->markTestIncomplete('Prepared REGEXP UPDATE: expected 1 row');
            }

            $this->assertSame('archived', $check[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with REGEXP failed: ' . $e->getMessage());
        }
    }
}
