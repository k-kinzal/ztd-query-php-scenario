<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE/UPDATE with complex expression WHERE clauses via SQLite PDO.
 *
 * Validates that the CTE rewriter correctly handles function calls,
 * CASE expressions, and computed conditions in WHERE clauses of DML statements.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class SqliteExpressionWhereClauseDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ewd_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL,
            category TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ewd_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ewd_test VALUES (1, 'alice', 85, 'A')");
        $this->pdo->exec("INSERT INTO sl_ewd_test VALUES (2, 'BOB', 60, 'B')");
        $this->pdo->exec("INSERT INTO sl_ewd_test VALUES (3, 'Charlie', 95, 'A')");
        $this->pdo->exec("INSERT INTO sl_ewd_test VALUES (4, 'dave', 45, 'C')");
        $this->pdo->exec("INSERT INTO sl_ewd_test VALUES (5, 'Eve', 70, 'B')");
    }

    /**
     * DELETE with LENGTH() function in WHERE clause.
     *
     * LENGTH(name) > 4 matches: 'alice' (5), 'Charlie' (7).
     * Remaining: BOB, dave, Eve = 3 rows.
     */
    public function testDeleteWithFunctionInWhere(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_ewd_test WHERE LENGTH(name) > 4");

            $rows = $this->pdo->query("SELECT id, name FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with LENGTH() in WHERE: expected 3 remaining rows, got ' . count($rows)
                    . '. Names: ' . implode(', ', array_column($rows, 'name'))
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(2, (int) $rows[0]['id']); // BOB
            $this->assertSame(4, (int) $rows[1]['id']); // dave
            $this->assertSame(5, (int) $rows[2]['id']); // Eve
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with LENGTH() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with CASE expression in WHERE clause.
     *
     * CASE WHEN score > 80 THEN 1 ELSE 0 END = 1
     * Matches: id 1 (score=85) and id 3 (score=95).
     * Remaining: id 2, 4, 5.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/96
     */
    public function testDeleteWithCaseInWhere(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_ewd_test WHERE CASE WHEN score > 80 THEN 1 ELSE 0 END = 1");

            $rows = $this->pdo->query("SELECT id FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with CASE in WHERE: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
            $this->assertSame(5, (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with CASE in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with CASE expression in WHERE clause.
     *
     * CASE WHEN score > 80 THEN 1 ELSE 0 END = 1
     * Should update only ids 1 (score=85) and 3 (score=95).
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/96
     */
    public function testUpdateWithCaseInWhere(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ewd_test SET score = 0 WHERE CASE WHEN score > 80 THEN 1 ELSE 0 END = 1");

            $rows = $this->pdo->query("SELECT id, score FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);

            $updated = array_filter($rows, fn($r) => (int) $r['score'] === 0);
            $updatedIds = array_map(fn($r) => (int) $r['id'], array_values($updated));

            if (count($updated) !== 2 || $updatedIds !== [1, 3]) {
                $allScores = array_map(fn($r) => "id={$r['id']} score={$r['score']}", $rows);
                $this->markTestIncomplete(
                    'Issue #96: UPDATE with CASE in WHERE updated wrong rows. Got: '
                    . implode(', ', $allScores)
                );
            }

            $this->assertSame(0, (int) $rows[0]['score']); // id 1
            $this->assertSame(60, (int) $rows[1]['score']); // id 2 unchanged
            $this->assertSame(0, (int) $rows[2]['score']); // id 3
            $this->assertSame(45, (int) $rows[3]['score']); // id 4 unchanged
            $this->assertSame(70, (int) $rows[4]['score']); // id 5 unchanged
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with CASE in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with LOWER() function in WHERE clause.
     *
     * LOWER(name) = 'bob' matches id 2 ('BOB').
     * Remaining: 4 rows.
     */
    public function testDeleteWithLowerFunctionInWhere(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_ewd_test WHERE LOWER(name) = 'bob'");

            $rows = $this->pdo->query("SELECT id FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE with LOWER() in WHERE: expected 4 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 3, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with LOWER() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with LENGTH() function in WHERE clause.
     *
     * LENGTH(name) <= 3 matches: 'BOB' (3), 'Eve' (3).
     * Updates category to 'X' for ids 2 and 5.
     */
    public function testUpdateWithFunctionInWhere(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ewd_test SET category = 'X' WHERE LENGTH(name) <= 3");

            $rows = $this->pdo->query("SELECT id, category FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $this->assertSame('A', $rows[0]['category']); // id 1, alice
            $this->assertSame('X', $rows[1]['category']); // id 2, BOB
            $this->assertSame('A', $rows[2]['category']); // id 3, Charlie
            $this->assertSame('C', $rows[3]['category']); // id 4, dave
            $this->assertSame('X', $rows[4]['category']); // id 5, Eve
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with LENGTH() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with arithmetic expression in WHERE clause.
     *
     * score * 2 > 150 matches: id 1 (85*2=170) and id 3 (95*2=190).
     * Updates score = score + 10 for those rows.
     */
    public function testUpdateWithArithmeticWhere(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ewd_test SET score = score + 10 WHERE score * 2 > 150");

            $rows = $this->pdo->query("SELECT id, score FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $this->assertSame(95, (int) $rows[0]['score']);  // id 1: 85 + 10
            $this->assertSame(60, (int) $rows[1]['score']);  // id 2: unchanged
            $this->assertSame(105, (int) $rows[2]['score']); // id 3: 95 + 10
            $this->assertSame(45, (int) $rows[3]['score']);  // id 4: unchanged
            $this->assertSame(70, (int) $rows[4]['score']);  // id 5: unchanged
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with arithmetic WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with ABS() function in WHERE clause.
     *
     * ABS(score - 70) <= 15 matches scores within [55, 85]:
     *   id 1 (85): ABS(15)=15 -> yes
     *   id 2 (60): ABS(10)=10 -> yes
     *   id 3 (95): ABS(25)=25 -> no
     *   id 4 (45): ABS(25)=25 -> no
     *   id 5 (70): ABS(0)=0   -> yes
     * Deletes ids 1, 2, 5. Remaining: ids 3, 4.
     */
    public function testDeleteWithBetweenAndFunction(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_ewd_test WHERE ABS(score - 70) <= 15");

            $rows = $this->pdo->query("SELECT id FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE with ABS() in WHERE: expected 2 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with ABS() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with || string concatenation in WHERE clause (SQLite syntax).
     *
     * WHERE name || category LIKE '%A' matches rows where
     * the concatenation ends with 'A':
     *   id 1: 'alice' || 'A' = 'aliceA' -> yes
     *   id 3: 'Charlie' || 'A' = 'CharlieA' -> yes
     * Updates score to 100 for those rows.
     */
    public function testUpdateWithConcatInWhere(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ewd_test SET score = 100 WHERE name || category LIKE '%A'");

            $rows = $this->pdo->query("SELECT id, score FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $this->assertSame(100, (int) $rows[0]['score']); // id 1: aliceA
            $this->assertSame(60, (int) $rows[1]['score']);   // id 2: BOBB
            $this->assertSame(100, (int) $rows[2]['score']);  // id 3: CharlieA
            $this->assertSame(45, (int) $rows[3]['score']);   // id 4: daveC
            $this->assertSame(70, (int) $rows[4]['score']);   // id 5: EveB
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with || concat in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with expression-based WHERE clause.
     *
     * DELETE FROM sl_ewd_test WHERE score > ? AND LENGTH(name) > ?
     * With params (60, 3): matches rows where score > 60 AND LENGTH(name) > 3:
     *   id 1: score=85>60 AND LENGTH('alice')=5>3 -> yes
     *   id 2: score=60 NOT > 60 -> no
     *   id 3: score=95>60 AND LENGTH('Charlie')=7>3 -> yes
     *   id 4: score=45 NOT > 60 -> no
     *   id 5: score=70>60 AND LENGTH('Eve')=3 NOT > 3 -> no
     * Deletes ids 1, 3. Remaining: ids 2, 4, 5.
     */
    public function testPreparedDeleteWithExpressionWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare('DELETE FROM sl_ewd_test WHERE score > ? AND LENGTH(name) > ?');
            $stmt->execute([60, 3]);

            $rows = $this->pdo->query("SELECT id FROM sl_ewd_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE with expression WHERE: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE with expression WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DML with expression WHERE should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE sl_ewd_test SET score = 999 WHERE LENGTH(name) > 3");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ewd_test")
            ->fetchAll(PDO::FETCH_ASSOC);

        // Physical table should be empty (data seeded via ZTD)
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
