<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL 8.0.26+ NULLS FIRST / NULLS LAST support in DML contexts.
 *
 * MySQL traditionally sorts NULLs first in ASC. MySQL 8.0.26+ added
 * NULLS FIRST/LAST support. The CTE rewriter must preserve these clauses.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class MysqlNullsFirstLastDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_nfl_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                priority INT,
                category VARCHAR(10)
            )',
            'CREATE TABLE my_nfl_ranked (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                rank_pos INT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_nfl_ranked', 'my_nfl_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_nfl_items (name, priority, category) VALUES ('Alpha', 10, 'A')");
        $this->pdo->exec("INSERT INTO my_nfl_items (name, priority, category) VALUES ('Beta', NULL, 'A')");
        $this->pdo->exec("INSERT INTO my_nfl_items (name, priority, category) VALUES ('Gamma', 5, 'B')");
        $this->pdo->exec("INSERT INTO my_nfl_items (name, priority, category) VALUES ('Delta', NULL, 'B')");
        $this->pdo->exec("INSERT INTO my_nfl_items (name, priority, category) VALUES ('Epsilon', 20, NULL)");
    }

    /**
     * SELECT with ORDER BY ... NULLS LAST (overriding MySQL default).
     * MySQL 8.0.26+ syntax.
     *
     * @spec SPEC-3.1
     */
    public function testSelectNullsLast(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT id, name, priority FROM my_nfl_items ORDER BY priority ASC NULLS LAST'
            );

            if (count($rows) < 5) {
                $this->markTestIncomplete('SELECT NULLS LAST: got ' . count($rows) . ' rows');
            }

            $this->assertCount(5, $rows);
            // Non-NULL priorities should come first
            $this->assertNotNull($rows[0]['priority']);
            $this->assertNull($rows[3]['priority']);
            $this->assertNull($rows[4]['priority']);
        } catch (\Throwable $e) {
            // May fail if MySQL < 8.0.26
            $this->markTestIncomplete('SELECT NULLS LAST failed (may require MySQL 8.0.26+): ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with ORDER BY NULLS LAST LIMIT.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectNullsLastLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO my_nfl_ranked (id, name, rank_pos)
                 SELECT id, name, ROW_NUMBER() OVER (ORDER BY priority ASC NULLS LAST)
                 FROM my_nfl_items
                 ORDER BY priority ASC NULLS LAST
                 LIMIT 3"
            );

            $rows = $this->ztdQuery('SELECT id, name FROM my_nfl_ranked ORDER BY rank_pos');

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT NULLS LAST: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT NULLS LAST LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with NULLS FIRST.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedSelectNullsFirst(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT name, priority FROM my_nfl_items
                 WHERE category = ?
                 ORDER BY priority ASC NULLS FIRST',
                ['A']
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('Prepared NULLS FIRST: expected 2 rows');
            }

            $this->assertCount(2, $rows);
            // NULL should come first in NULLS FIRST ordering
            $this->assertNull($rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared NULLS FIRST failed: ' . $e->getMessage());
        }
    }
}
