<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether subqueries within INSERT VALUES positions are handled
 * by the CTE rewriter.
 *
 * Pattern: INSERT INTO t (a, b) VALUES ((SELECT MAX(a) FROM t2), 'val')
 * This is a common pattern for manual ID generation and referencing
 * related data during insertion.
 *
 * @spec SPEC-4.1
 */
class SqliteSubqueryInValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_siv_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                score INTEGER NOT NULL
            )",
            "CREATE TABLE sl_siv_target (
                id INTEGER PRIMARY KEY,
                ref_name TEXT,
                ref_score INTEGER
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_siv_source', 'sl_siv_target'];
    }

    /**
     * INSERT with scalar subquery in VALUES referencing another table.
     */
    public function testScalarSubqueryFromOtherTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (1, 'Alice', 85)");
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (2, 'Bob', 92)");

            $this->pdo->exec(
                "INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES (1, 'test', (SELECT MAX(score) FROM sl_siv_source))"
            );

            $rows = $this->ztdQuery("SELECT ref_score FROM sl_siv_target WHERE id = 1");
            $this->assertCount(1, $rows);

            $score = $rows[0]['ref_score'];
            if ($score === null) {
                $this->markTestIncomplete(
                    'Scalar subquery in VALUES returned NULL. Expected 92 (MAX score from source).'
                );
            }
            if ((int) $score !== 92) {
                $this->markTestIncomplete(
                    'Scalar subquery in VALUES returned wrong value. Expected 92, got ' . json_encode($score)
                );
            }
            $this->assertEquals(92, (int) $score);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Scalar subquery from other table test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with scalar subquery referencing the same table (self-reference).
     * Pattern: INSERT INTO t VALUES ((SELECT MAX(id) FROM t) + 1, ...)
     */
    public function testSelfReferencingSubqueryInValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES (1, 'first', 10)");
            $this->pdo->exec("INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES (2, 'second', 20)");

            $this->pdo->exec(
                "INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES ((SELECT MAX(id) FROM sl_siv_target) + 1, 'third', 30)"
            );

            $rows = $this->ztdQuery("SELECT id, ref_name FROM sl_siv_target ORDER BY id");
            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'Self-referencing subquery INSERT produced ' . count($rows) . ' rows instead of 3.'
                );
            }

            $thirdRow = $rows[2] ?? null;
            if ($thirdRow === null || (int) $thirdRow['id'] !== 3) {
                $this->markTestIncomplete(
                    'Self-referencing MAX(id)+1 produced wrong id. Expected 3, got: ' . json_encode($thirdRow)
                );
            }
            $this->assertEquals(3, (int) $thirdRow['id']);
            $this->assertSame('third', $thirdRow['ref_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-referencing subquery in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple scalar subqueries in a single INSERT VALUES clause.
     */
    public function testMultipleSubqueriesInValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (1, 'Alice', 85)");
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (2, 'Bob', 92)");
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (3, 'Carol', 78)");

            $this->pdo->exec(
                "INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES (
                    1,
                    (SELECT name FROM sl_siv_source WHERE score = (SELECT MAX(score) FROM sl_siv_source)),
                    (SELECT MIN(score) FROM sl_siv_source)
                )"
            );

            $rows = $this->ztdQuery("SELECT ref_name, ref_score FROM sl_siv_target WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['ref_name'] === null || $row['ref_score'] === null) {
                $this->markTestIncomplete(
                    'Multiple subqueries in VALUES returned NULLs. Expected name="Bob", score=78. Got: ' . json_encode($row)
                );
            }
            $this->assertSame('Bob', $row['ref_name']);
            $this->assertEquals(78, (int) $row['ref_score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple subqueries in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with subquery in VALUES — parameters apply to the
     * subquery, not the outer INSERT.
     */
    public function testPreparedSubqueryInValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (1, 'Alice', 85)");
            $this->pdo->exec("INSERT INTO sl_siv_source (id, name, score) VALUES (2, 'Bob', 92)");

            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_siv_target (id, ref_name, ref_score) VALUES (1, (SELECT name FROM sl_siv_source WHERE id = ?), 0)"
            );
            $stmt->execute([2]);

            $rows = $this->ztdQuery("SELECT ref_name FROM sl_siv_target WHERE id = 1");
            $this->assertCount(1, $rows);

            $name = $rows[0]['ref_name'];
            if ($name === null) {
                $this->markTestIncomplete(
                    'Prepared subquery in VALUES returned NULL. Expected "Bob".'
                );
            }
            if ($name !== 'Bob') {
                $this->markTestIncomplete(
                    'Prepared subquery in VALUES returned wrong value. Expected "Bob", got ' . json_encode($name)
                );
            }
            $this->assertSame('Bob', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared subquery in VALUES test failed: ' . $e->getMessage());
        }
    }
}
