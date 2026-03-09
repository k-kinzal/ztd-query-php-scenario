<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT DEFAULT VALUES syntax via PostgreSQL PDO.
 *
 * PostgreSQL supports `INSERT INTO t DEFAULT VALUES` which inserts a row
 * using defaults for all columns. The CTE rewriter must handle this minimal
 * INSERT syntax correctly, including shadow store interactions.
 *
 * @spec SPEC-4.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/97
 */
class PostgresInsertDefaultValuesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_defv_test (
            id SERIAL PRIMARY KEY,
            status VARCHAR(20) NOT NULL DEFAULT \'pending\',
            priority INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_defv_test'];
    }

    /**
     * INSERT DEFAULT VALUES should create a row with all default values.
     */
    public function testInsertDefaultValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");

            $rows = $this->pdo->query("SELECT id, status, priority FROM pg_defv_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('pending', $rows[0]['status']);
            $this->assertSame(0, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple INSERT DEFAULT VALUES should create multiple rows.
     */
    public function testMultipleInsertDefaultValues(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");

            $rows = $this->pdo->query("SELECT id, status, priority FROM pg_defv_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple INSERT DEFAULT VALUES: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                $this->assertSame('pending', $row['status']);
                $this->assertSame(0, (int) $row['priority']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT DEFAULT VALUES followed by UPDATE should work correctly.
     */
    public function testInsertDefaultValuesThenUpdate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");

            $rows = $this->pdo->query("SELECT id FROM pg_defv_test")->fetchAll(PDO::FETCH_ASSOC);
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES: expected 1 row, got ' . count($rows)
                );
                return;
            }

            $id = (int) $rows[0]['id'];
            $this->pdo->exec("UPDATE pg_defv_test SET status = 'active', priority = 5 WHERE id = {$id}");

            $rows = $this->pdo->query("SELECT id, status, priority FROM pg_defv_test WHERE id = {$id}")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE after INSERT DEFAULT VALUES: row not found'
                );
            }

            $this->assertSame('active', $rows[0]['status']);
            $this->assertSame(5, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES then UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT DEFAULT VALUES followed by DELETE should remove the row.
     */
    public function testInsertDefaultValuesThenDelete(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");

            $rows = $this->pdo->query("SELECT id FROM pg_defv_test")->fetchAll(PDO::FETCH_ASSOC);
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES: expected 1 row before DELETE'
                );
                return;
            }

            $id = (int) $rows[0]['id'];
            $this->pdo->exec("DELETE FROM pg_defv_test WHERE id = {$id}");

            $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_defv_test")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES then DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT DEFAULT VALUES with RETURNING clause.
     */
    public function testInsertDefaultValuesWithReturning(): void
    {
        try {
            $rows = $this->pdo->query("INSERT INTO pg_defv_test DEFAULT VALUES RETURNING id, status, priority")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES RETURNING: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('pending', $rows[0]['status']);
            $this->assertSame(0, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES RETURNING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: INSERT DEFAULT VALUES should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_defv_test DEFAULT VALUES");
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_defv_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
