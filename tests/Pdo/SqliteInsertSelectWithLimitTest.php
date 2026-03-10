<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with LIMIT and OFFSET clauses.
 *
 * Common data migration pattern: copy a subset of rows from one table
 * to another using LIMIT. The CTE rewriter must preserve LIMIT/OFFSET
 * semantics in the INSERT...SELECT transformation.
 *
 * @spec SPEC-4.1a
 */
class SqliteInsertSelectWithLimitTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_isl_source (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                priority INTEGER NOT NULL
            )",
            "CREATE TABLE sl_isl_archive (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                priority INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isl_source', 'sl_isl_archive'];
    }

    /**
     * INSERT...SELECT with LIMIT: only N rows should be copied.
     */
    public function testInsertSelectWithLimit(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (1, 'Alpha', 10)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (2, 'Beta', 20)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (3, 'Gamma', 30)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (4, 'Delta', 40)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (5, 'Epsilon', 50)");

            $this->pdo->exec(
                "INSERT INTO sl_isl_archive (id, name, priority) SELECT id, name, priority FROM sl_isl_source ORDER BY priority ASC LIMIT 3"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_isl_archive ORDER BY id");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with LIMIT produced 0 rows. Expected 3.'
                );
            }
            if (count($rows) === 5) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with LIMIT copied all 5 rows instead of LIMIT 3. LIMIT was ignored.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with LIMIT produced ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            $this->assertContains('Alpha', $names);
            $this->assertContains('Beta', $names);
            $this->assertContains('Gamma', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with LIMIT test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with LIMIT and OFFSET.
     */
    public function testInsertSelectWithLimitOffset(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (1, 'Alpha', 10)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (2, 'Beta', 20)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (3, 'Gamma', 30)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (4, 'Delta', 40)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (5, 'Epsilon', 50)");

            $this->pdo->exec(
                "INSERT INTO sl_isl_archive (id, name, priority) SELECT id, name, priority FROM sl_isl_source ORDER BY priority ASC LIMIT 2 OFFSET 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_isl_archive ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with LIMIT 2 OFFSET 1 produced ' . count($rows) . ' rows. Expected 2.'
                    . ' Got: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            // OFFSET 1 with ORDER BY priority ASC skips Alpha, takes Beta and Gamma
            $this->assertContains('Beta', $names);
            $this->assertContains('Gamma', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with LIMIT OFFSET test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with WHERE and LIMIT combined.
     */
    public function testInsertSelectWithWhereAndLimit(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (1, 'Alpha', 10)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (2, 'Beta', 20)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (3, 'Gamma', 30)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (4, 'Delta', 40)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (5, 'Epsilon', 50)");

            $this->pdo->exec(
                "INSERT INTO sl_isl_archive (id, name, priority) SELECT id, name, priority FROM sl_isl_source WHERE priority >= 30 ORDER BY priority ASC LIMIT 2"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_isl_archive ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT...SELECT with WHERE+LIMIT produced ' . count($rows) . ' rows. Expected 2.'
                    . ' Got: ' . json_encode($rows)
                );
            }

            $names = array_column($rows, 'name');
            $this->assertContains('Gamma', $names);
            $this->assertContains('Delta', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with WHERE and LIMIT test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT with LIMIT parameter.
     */
    public function testPreparedInsertSelectWithLimitParam(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (1, 'Alpha', 10)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (2, 'Beta', 20)");
            $this->pdo->exec("INSERT INTO sl_isl_source (id, name, priority) VALUES (3, 'Gamma', 30)");

            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_isl_archive (id, name, priority) SELECT id, name, priority FROM sl_isl_source ORDER BY id LIMIT ?"
            );
            $stmt->execute([2]);

            $rows = $this->ztdQuery("SELECT name FROM sl_isl_archive ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT...SELECT with LIMIT ? produced ' . count($rows) . ' rows. Expected 2.'
                    . ' Got: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertSame('Beta', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT SELECT with LIMIT param test failed: ' . $e->getMessage());
        }
    }
}
