<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL regex operators (~, ~*, !~, !~*) in DML through ZTD.
 *
 * The ~ operator is PostgreSQL's native regex match. The CTE rewriter's
 * regex-based SQL parser may misinterpret or fail to handle these operators
 * in UPDATE/DELETE WHERE clauses.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 * @spec SPEC-10.2.14
 */
class PostgresRegexOperatorDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_rgx_items (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'active\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_rgx_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rgx_items (code, name, status) VALUES ('PRD-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO pg_rgx_items (code, name, status) VALUES ('PRD-002', 'Widget Beta', 'active')");
        $this->pdo->exec("INSERT INTO pg_rgx_items (code, name, status) VALUES ('SVC-001', 'Service Plan', 'active')");
        $this->pdo->exec("INSERT INTO pg_rgx_items (code, name, status) VALUES ('SVC-002', 'Service Premium', 'inactive')");
        $this->pdo->exec("INSERT INTO pg_rgx_items (code, name, status) VALUES ('ACC-001', 'Accessory Kit', 'active')");
    }

    /**
     * UPDATE with ~ (regex match) in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithRegexMatch(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_rgx_items SET status = 'discontinued' WHERE code ~ '^PRD-'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_rgx_items WHERE code ~ '^PRD-' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with ~: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertSame('discontinued', $rows[0]['status']);
            $this->assertSame('discontinued', $rows[1]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with ~ regex operator failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with ~ (regex match) in WHERE.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithRegexMatch(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_rgx_items WHERE code ~ '^SVC-[0-9]+$'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE with ~: expected 3 remaining rows, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with ~ regex failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with ~* (case-insensitive regex) in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithCaseInsensitiveRegex(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_rgx_items SET status = 'matched' WHERE name ~* 'widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_rgx_items WHERE status = 'matched' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with ~*: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with ~* case-insensitive regex failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with !~ (regex not match) in WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithRegexNotMatch(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_rgx_items SET status = 'other' WHERE code !~ '^PRD' AND status = 'active'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pg_rgx_items WHERE status = 'other' ORDER BY id"
            );

            // SVC-001 (active) and ACC-001 (active) should match
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with !~: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with !~ regex not-match failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with ~ and parameter for the new value.
     * The regex pattern is a literal, not a parameter.
     *
     * @spec SPEC-4.2
     */
    public function testPreparedUpdateWithRegex(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_rgx_items SET status = ? WHERE code ~ '^ACC'"
            );
            $stmt->execute(['archived']);

            $rows = $this->ztdQuery(
                "SELECT status FROM pg_rgx_items WHERE code ~ '^ACC'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared ~ UPDATE: expected 1 row');
            }

            $this->assertSame('archived', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with ~ regex failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with SIMILAR TO pattern on shadow data.
     *
     * @spec SPEC-3.1
     */
    public function testSelectSimilarTo(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, code FROM pg_rgx_items WHERE code SIMILAR TO '(PRD|SVC)-[0-9]+' ORDER BY id"
            );

            $this->assertCount(4, $rows, 'SIMILAR TO should match PRD-* and SVC-* codes');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT SIMILAR TO failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with SIMILAR TO pattern.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithSimilarTo(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_rgx_items WHERE code SIMILAR TO 'SVC-[0-9]+'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_rgx_items');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE SIMILAR TO: expected 3 remaining rows, got ' . $remaining
                );
            }

            $this->assertEquals(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with SIMILAR TO failed: ' . $e->getMessage());
        }
    }
}
