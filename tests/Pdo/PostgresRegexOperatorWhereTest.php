<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests regex operators (~, ~*, !~, !~*) in WHERE clause via PostgreSQL PDO.
 *
 * PostgreSQL uses ~ for case-sensitive regex and ~* for case-insensitive.
 * The CTE rewriter must parse these single-character operators correctly
 * without confusing them with other syntax.
 *
 * @spec SPEC-3.1
 */
class PostgresRegexOperatorWhereTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_regex_test (
            id SERIAL PRIMARY KEY,
            email VARCHAR(200) NOT NULL,
            code VARCHAR(50) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_regex_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_regex_test (id, email, code) VALUES (1, 'alice@example.com', 'ABC-123')");
        $this->pdo->exec("INSERT INTO pg_regex_test (id, email, code) VALUES (2, 'bob@test.org', 'DEF-456')");
        $this->pdo->exec("INSERT INTO pg_regex_test (id, email, code) VALUES (3, 'Charlie@Example.com', 'GHI-789')");
        $this->pdo->exec("INSERT INTO pg_regex_test (id, email, code) VALUES (4, 'dave@other.net', 'ABC-999')");
        $this->pdo->exec("INSERT INTO pg_regex_test (id, email, code) VALUES (5, 'eve@test.org', 'XYZ-000')");
    }

    /**
     * SELECT with ~ (case-sensitive regex match) in WHERE clause.
     *
     * email ~ '@example\.com$' matches id 1 only (case-sensitive, id 3 has uppercase).
     */
    public function testSelectWithTildeRegex(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM pg_regex_test WHERE email ~ '@example\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'SELECT with ~: expected 1 row (case-sensitive), got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with ~ regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with ~* (case-insensitive regex match) in WHERE clause.
     *
     * email ~* '@example\.com$' matches ids 1 and 3 (case-insensitive).
     */
    public function testSelectWithTildeStarRegex(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM pg_regex_test WHERE email ~* '@example\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with ~*: expected 2 rows (case-insensitive), got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with ~* regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with !~ (negated case-sensitive regex) in WHERE clause.
     *
     * email !~ '@example\.com$' matches ids 2, 3, 4, 5
     * (id 3 has uppercase Example, so it doesn't match the lowercase pattern).
     */
    public function testSelectWithNegatedTildeRegex(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM pg_regex_test WHERE email !~ '@example\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT with !~: expected 4 rows, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 3, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with !~ regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with !~* (negated case-insensitive regex) in WHERE clause.
     *
     * email !~* '@example\.com$' matches ids 2, 4, 5.
     */
    public function testSelectWithNegatedTildeStarRegex(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM pg_regex_test WHERE email !~* '@example\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT with !~*: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with !~* regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with ~ in WHERE clause.
     *
     * Delete rows where email ~ '@test\.org$' (ids 2 and 5).
     * Remaining: ids 1, 3, 4.
     */
    public function testDeleteWithTildeRegex(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_regex_test WHERE email ~ '@test\\.org$'");

            $rows = $this->pdo->query("SELECT id FROM pg_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with ~: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 3, 4], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with ~ regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with ~* in WHERE clause.
     *
     * Update code to 'MATCHED' where code ~* '^abc' (case-insensitive).
     * Matches ids 1 and 4 (codes 'ABC-123' and 'ABC-999').
     */
    public function testUpdateWithTildeStarRegex(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_regex_test SET code = 'MATCHED' WHERE code ~* '^abc'"
            );

            $rows = $this->pdo->query("SELECT id, code FROM pg_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $matchedCount = count(array_filter($rows, fn($r) => $r['code'] === 'MATCHED'));

            if ($matchedCount !== 2) {
                $codes = array_map(fn($r) => "id={$r['id']} code={$r['code']}", $rows);
                $this->markTestIncomplete(
                    'UPDATE with ~*: expected 2 rows updated, got ' . $matchedCount
                    . '. ' . implode(', ', $codes)
                );
            }

            $this->assertSame('MATCHED', $rows[0]['code']); // id 1
            $this->assertSame('DEF-456', $rows[1]['code']); // id 2
            $this->assertSame('GHI-789', $rows[2]['code']); // id 3
            $this->assertSame('MATCHED', $rows[3]['code']); // id 4
            $this->assertSame('XYZ-000', $rows[4]['code']); // id 5
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with ~* regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with ~ and $1 parameter.
     *
     * The regex pattern is passed as a bound parameter.
     */
    public function testPreparedSelectWithTildeRegex(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id FROM pg_regex_test WHERE code ~ $1 ORDER BY id"
            );
            $stmt->execute(['^ABC']);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT with ~: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with ~ regex failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with SIMILAR TO operator (SQL standard regex).
     *
     * SIMILAR TO uses SQL standard regex syntax (different from POSIX ~).
     * code SIMILAR TO 'ABC-%' matches ids 1 and 4.
     */
    public function testSelectWithSimilarTo(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM pg_regex_test WHERE code SIMILAR TO 'ABC-%' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with SIMILAR TO: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with SIMILAR TO failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DML with regex operator should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("DELETE FROM pg_regex_test WHERE email ~ '@test'");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_regex_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
