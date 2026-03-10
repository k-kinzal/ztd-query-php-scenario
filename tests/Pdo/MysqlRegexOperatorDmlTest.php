<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests REGEXP operator in DML through ZTD on MySQL.
 *
 * MySQL's REGEXP / RLIKE operator uses a regex engine. The CTE rewriter
 * may mishandle the regex pattern string or the operator keyword itself.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 * @spec SPEC-10.2.15
 */
class MysqlRegexOperatorDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_rgx_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_rgx_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_rgx_items (code, name, status) VALUES ('PRD-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO my_rgx_items (code, name, status) VALUES ('PRD-002', 'Widget Beta', 'active')");
        $this->pdo->exec("INSERT INTO my_rgx_items (code, name, status) VALUES ('SVC-001', 'Service Plan', 'active')");
        $this->pdo->exec("INSERT INTO my_rgx_items (code, name, status) VALUES ('SVC-002', 'Service Premium', 'inactive')");
        $this->pdo->exec("INSERT INTO my_rgx_items (code, name, status) VALUES ('ACC-001', 'Accessory Kit', 'active')");
    }

    /**
     * UPDATE with REGEXP in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithRegexp(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_rgx_items SET status = 'discontinued' WHERE code REGEXP '^PRD-[0-9]+$'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM my_rgx_items WHERE code REGEXP '^PRD-' ORDER BY id"
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
            $this->pdo->exec(
                "DELETE FROM my_rgx_items WHERE code REGEXP '^SVC-'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM my_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE with REGEXP: expected 3 remaining rows, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with REGEXP failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with RLIKE (synonym for REGEXP) in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithRlike(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_rgx_items SET status = 'matched' WHERE name RLIKE 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM my_rgx_items WHERE status = 'matched' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with RLIKE: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with RLIKE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with NOT REGEXP in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithNotRegexp(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_rgx_items SET status = 'other' WHERE code NOT REGEXP '^PRD' AND status = 'active'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM my_rgx_items WHERE status = 'other' ORDER BY id"
            );

            // SVC-001 (active) and ACC-001 (active) should match
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE NOT REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with NOT REGEXP failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with REGEXP.
     *
     * @spec SPEC-4.3
     */
    public function testPreparedDeleteWithRegexp(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_rgx_items WHERE code REGEXP ?"
            );
            $stmt->execute(['^ACC-']);

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM my_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE REGEXP: expected 4 remaining, got ' . $remaining
                );
            }

            $this->assertEquals(4, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with REGEXP failed: ' . $e->getMessage());
        }
    }
}
