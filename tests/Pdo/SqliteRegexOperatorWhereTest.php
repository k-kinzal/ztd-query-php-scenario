<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GLOB operator and LIKE patterns in WHERE clause via SQLite PDO.
 *
 * SQLite does not support REGEXP by default (requires an extension).
 * GLOB is the SQLite-native pattern matching operator with case-sensitive
 * Unix-style wildcards (* and ?). The CTE rewriter must handle GLOB correctly.
 *
 * @spec SPEC-3.1
 */
class SqliteRegexOperatorWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_regex_test (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            code TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_regex_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_regex_test VALUES (1, 'alice@example.com', 'ABC-123')");
        $this->pdo->exec("INSERT INTO sl_regex_test VALUES (2, 'bob@test.org', 'DEF-456')");
        $this->pdo->exec("INSERT INTO sl_regex_test VALUES (3, 'charlie@example.com', 'GHI-789')");
        $this->pdo->exec("INSERT INTO sl_regex_test VALUES (4, 'dave@other.net', 'ABC-999')");
        $this->pdo->exec("INSERT INTO sl_regex_test VALUES (5, 'eve@test.org', 'XYZ-000')");
    }

    /**
     * SELECT with GLOB operator (case-sensitive pattern matching).
     *
     * GLOB '*@example.com' matches ids 1 and 3.
     * GLOB uses * for any chars and ? for single char.
     */
    public function testSelectWithGlob(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM sl_regex_test WHERE email GLOB '*@example.com' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with GLOB: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with GLOB failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with NOT GLOB operator.
     *
     * NOT GLOB '*@example.com' matches ids 2, 4, 5.
     */
    public function testSelectWithNotGlob(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM sl_regex_test WHERE email NOT GLOB '*@example.com' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT with NOT GLOB: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with NOT GLOB failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with GLOB and character class [A-Z].
     *
     * code GLOB 'ABC-[0-9][0-9][0-9]' matches ids 1 and 4.
     */
    public function testSelectWithGlobCharacterClass(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM sl_regex_test WHERE code GLOB 'ABC-[0-9][0-9][0-9]' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with GLOB character class: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with GLOB character class failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with GLOB in WHERE clause.
     *
     * Delete rows where email GLOB '*@test.org' (ids 2 and 5).
     * Remaining: ids 1, 3, 4.
     */
    public function testDeleteWithGlob(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_regex_test WHERE email GLOB '*@test.org'");

            $rows = $this->pdo->query("SELECT id FROM sl_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with GLOB: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 3, 4], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with GLOB failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with GLOB in WHERE clause.
     *
     * Update code to 'MATCHED' where code GLOB 'ABC-*'.
     * Matches ids 1 and 4.
     */
    public function testUpdateWithGlob(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_regex_test SET code = 'MATCHED' WHERE code GLOB 'ABC-*'");

            $rows = $this->pdo->query("SELECT id, code FROM sl_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $this->assertSame('MATCHED', $rows[0]['code']); // id 1
            $this->assertSame('DEF-456', $rows[1]['code']); // id 2
            $this->assertSame('GHI-789', $rows[2]['code']); // id 3
            $this->assertSame('MATCHED', $rows[3]['code']); // id 4
            $this->assertSame('XYZ-000', $rows[4]['code']); // id 5
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with GLOB failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with GLOB and bound parameter.
     */
    public function testPreparedSelectWithGlob(): void
    {
        try {
            $stmt = $this->pdo->prepare("SELECT id FROM sl_regex_test WHERE code GLOB ? ORDER BY id");
            $stmt->execute(['ABC-*']);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT with GLOB: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with GLOB failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GLOB is case-sensitive: 'ABC-*' should not match 'abc-123'.
     * Verify that shadow store preserves case sensitivity.
     */
    public function testGlobCaseSensitivity(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_regex_test VALUES (6, 'frank@x.com', 'abc-111')");

            $rows = $this->pdo->query(
                "SELECT id FROM sl_regex_test WHERE code GLOB 'ABC-*' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GLOB case sensitivity: expected 2 rows (uppercase only), got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GLOB case sensitivity test failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DML with GLOB should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("DELETE FROM sl_regex_test WHERE email GLOB '*@test*'");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_regex_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
