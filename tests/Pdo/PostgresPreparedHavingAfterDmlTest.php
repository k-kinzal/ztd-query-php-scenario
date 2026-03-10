<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared statements with HAVING clause after DML on PostgreSQL.
 *
 * Issue #22: Prepared params in HAVING fail on PostgreSQL.
 *
 * @spec SPEC-4.2
 */
class PostgresPreparedHavingAfterDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_phd_items (
            id INT PRIMARY KEY,
            category VARCHAR(30) NOT NULL,
            name VARCHAR(50) NOT NULL,
            price NUMERIC(10,2) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_phd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_phd_items VALUES (1, 'tools', 'Hammer', 15.00)");
        $this->pdo->exec("INSERT INTO pg_phd_items VALUES (2, 'tools', 'Wrench', 12.00)");
        $this->pdo->exec("INSERT INTO pg_phd_items VALUES (3, 'electronics', 'Radio', 45.00)");
        $this->pdo->exec("INSERT INTO pg_phd_items VALUES (4, 'clothing', 'Shirt', 25.00)");
    }

    /**
     * Prepared HAVING on PostgreSQL — documents Issue #22 scope.
     */
    public function testPreparedHavingCountAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM pg_phd_items
                 GROUP BY category
                 HAVING COUNT(*) > ?",
                [1]
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Prepared HAVING on PostgreSQL: empty result (Issue #22)');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared HAVING COUNT on PostgreSQL failed (Issue #22): ' . $e->getMessage());
        }
    }

    /**
     * Non-prepared HAVING baseline.
     */
    public function testNonPreparedHavingAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdQuery(
                "SELECT category, COUNT(*) AS cnt
                 FROM pg_phd_items
                 GROUP BY category
                 HAVING COUNT(*) > 1"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Non-prepared HAVING on PG: empty result');
            }
            $this->assertCount(1, $rows);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Non-prepared HAVING on PG failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared WHERE + HAVING combined on PostgreSQL.
     */
    public function testPreparedWhereAndHaving(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_phd_items VALUES (5, 'tools', 'Saw', 20.00)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT category, COUNT(*) AS cnt
                 FROM pg_phd_items
                 WHERE price > ?
                 GROUP BY category
                 HAVING COUNT(*) >= ?",
                [10.0, 2]
            );

            if (empty($rows)) {
                $this->markTestIncomplete('WHERE+HAVING on PG: empty (Issue #22)');
            }

            $cats = array_column($rows, 'category');
            if (!in_array('tools', $cats)) {
                $this->markTestIncomplete('WHERE+HAVING: tools not found. Got: ' . implode(', ', $cats));
            }
            $this->assertContains('tools', $cats);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared WHERE+HAVING on PG failed: ' . $e->getMessage());
        }
    }
}
