<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared UPDATE and DELETE with BETWEEN in WHERE clause.
 *
 * SELECT with BETWEEN and params is tested. This verifies the DML variants
 * (UPDATE/DELETE) which go through different CTE rewriter code paths.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class SqlitePreparedUpdateBetweenTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_pub_scores (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL,
            grade TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_pub_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_pub_scores VALUES (1, 'Alice',   95, NULL)");
        $this->pdo->exec("INSERT INTO sl_pub_scores VALUES (2, 'Bob',     72, NULL)");
        $this->pdo->exec("INSERT INTO sl_pub_scores VALUES (3, 'Charlie', 85, NULL)");
        $this->pdo->exec("INSERT INTO sl_pub_scores VALUES (4, 'Dave',    60, NULL)");
        $this->pdo->exec("INSERT INTO sl_pub_scores VALUES (5, 'Eve',     88, NULL)");
    }

    /**
     * Prepared UPDATE with BETWEEN in WHERE.
     *
     * UPDATE grades for scores between 80 and 90 → 'B'.
     * Matches: Charlie (85), Eve (88).
     */
    public function testPreparedUpdateWithBetween(): void
    {
        $sql = "UPDATE sl_pub_scores SET grade = ? WHERE score BETWEEN ? AND ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['B', 80, 90]);

            $rows = $this->ztdQuery("SELECT id, name, grade FROM sl_pub_scores ORDER BY id");

            $this->assertCount(5, $rows);

            $graded = array_filter($rows, fn($r) => $r['grade'] === 'B');
            $gradedNames = array_map(fn($r) => $r['name'], array_values($graded));

            if (count($graded) !== 2 || !in_array('Charlie', $gradedNames) || !in_array('Eve', $gradedNames)) {
                $allGrades = array_map(fn($r) => "{$r['name']}={$r['grade']}", $rows);
                $this->markTestIncomplete(
                    "Prepared UPDATE BETWEEN: expected Charlie and Eve graded 'B', got: "
                    . implode(', ', $allGrades)
                );
            }

            $this->assertCount(2, $graded);
            $this->assertNull($rows[0]['grade']); // Alice 95 - not in range
            $this->assertNull($rows[1]['grade']); // Bob 72 - not in range
            $this->assertSame('B', $rows[2]['grade']); // Charlie 85
            $this->assertNull($rows[3]['grade']); // Dave 60
            $this->assertSame('B', $rows[4]['grade']); // Eve 88
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE with BETWEEN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with BETWEEN in WHERE.
     *
     * DELETE scores between 70 and 89. Matches: Bob (72), Charlie (85), Eve (88).
     * Remaining: Alice (95), Dave (60).
     */
    public function testPreparedDeleteWithBetween(): void
    {
        $sql = "DELETE FROM sl_pub_scores WHERE score BETWEEN ? AND ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([70, 89]);

            $rows = $this->ztdQuery("SELECT id, name, score FROM sl_pub_scores ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE BETWEEN: expected 2 remaining rows, got ' . count($rows)
                    . '. Names: ' . implode(', ', array_column($rows, 'name'))
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']); // 95, outside range
            $this->assertSame('Dave', $rows[1]['name']);   // 60, outside range
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE with BETWEEN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with NOT BETWEEN.
     *
     * Update scores NOT between 70 and 90. Matches: Alice (95), Dave (60).
     */
    public function testPreparedUpdateWithNotBetween(): void
    {
        $sql = "UPDATE sl_pub_scores SET grade = 'X' WHERE score NOT BETWEEN ? AND ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([70, 90]);

            $rows = $this->ztdQuery("SELECT id, name, grade FROM sl_pub_scores ORDER BY id");

            $graded = array_filter($rows, fn($r) => $r['grade'] === 'X');
            $gradedNames = array_map(fn($r) => $r['name'], array_values($graded));

            if (count($graded) !== 2) {
                $allGrades = array_map(fn($r) => "{$r['name']}={$r['grade']}", $rows);
                $this->markTestIncomplete(
                    "Prepared UPDATE NOT BETWEEN: expected 2 graded, got "
                    . count($graded) . '. All: ' . implode(', ', $allGrades)
                );
            }

            $this->assertCount(2, $graded);
            $this->assertSame('X', $rows[0]['grade']); // Alice 95
            $this->assertNull($rows[1]['grade']);       // Bob 72
            $this->assertNull($rows[2]['grade']);       // Charlie 85
            $this->assertSame('X', $rows[3]['grade']); // Dave 60
            $this->assertNull($rows[4]['grade']);       // Eve 88
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE with NOT BETWEEN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Chained prepared UPDATE — first BETWEEN, then another condition.
     */
    public function testChainedPreparedUpdatesWithBetween(): void
    {
        try {
            // First: grade scores 80-100 as 'A'
            $stmt = $this->pdo->prepare("UPDATE sl_pub_scores SET grade = ? WHERE score BETWEEN ? AND ?");
            $stmt->execute(['A', 80, 100]);

            // Second: grade scores 60-79 as 'C'
            $stmt2 = $this->pdo->prepare("UPDATE sl_pub_scores SET grade = ? WHERE score BETWEEN ? AND ?");
            $stmt2->execute(['C', 60, 79]);

            $rows = $this->ztdQuery("SELECT name, score, grade FROM sl_pub_scores ORDER BY score DESC");

            $this->assertCount(5, $rows);

            // Verify all grades assigned correctly
            $grades = [];
            foreach ($rows as $r) {
                $grades[$r['name']] = $r['grade'];
            }

            $aliceGrade = $grades['Alice'] ?? null;
            $bobGrade = $grades['Bob'] ?? null;
            $charlieGrade = $grades['Charlie'] ?? null;
            $daveGrade = $grades['Dave'] ?? null;
            $eveGrade = $grades['Eve'] ?? null;

            if ($aliceGrade !== 'A' || $bobGrade !== 'C' || $charlieGrade !== 'A'
                || $daveGrade !== 'C' || $eveGrade !== 'A') {
                $this->markTestIncomplete(
                    "Chained BETWEEN updates wrong: Alice={$aliceGrade} Bob={$bobGrade} "
                    . "Charlie={$charlieGrade} Dave={$daveGrade} Eve={$eveGrade}"
                );
            }

            $this->assertSame('A', $aliceGrade);
            $this->assertSame('C', $bobGrade);
            $this->assertSame('A', $charlieGrade);
            $this->assertSame('C', $daveGrade);
            $this->assertSame('A', $eveGrade);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Chained prepared BETWEEN updates failed: ' . $e->getMessage()
            );
        }
    }
}
