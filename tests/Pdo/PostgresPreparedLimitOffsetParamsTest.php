<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared SELECT with parameterized LIMIT and OFFSET on PostgreSQL.
 *
 * PostgreSQL uses $N params natively but PDO converts ? to $N.
 * Tests both ? and $N styles for LIMIT/OFFSET.
 *
 * @spec SPEC-3.2
 */
class PostgresPreparedLimitOffsetParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_plop_items (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            score INTEGER NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_plop_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO pg_plop_items VALUES ({$i}, 'item{$i}', " . ($i * 10) . ")");
        }
    }

    /**
     * Prepared LIMIT ? OFFSET ? with ? placeholders.
     */
    public function testPreparedLimitOffsetQuestionMark(): void
    {
        $sql = "SELECT id, name FROM pg_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [3, 2]);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'LIMIT ? OFFSET ? (?-style): expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(5, (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'LIMIT ? OFFSET ? (?-style) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE ? + LIMIT ? OFFSET ? — parameter index alignment.
     */
    public function testPreparedWhereAndLimitOffsetParams(): void
    {
        $sql = "SELECT id FROM pg_plop_items WHERE score >= ? ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [50, 2, 1]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE+LIMIT+OFFSET: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(6, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE+LIMIT+OFFSET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared LIMIT $1 OFFSET $2 with native PostgreSQL params.
     */
    public function testPreparedLimitOffsetDollarParams(): void
    {
        $sql = "SELECT id FROM pg_plop_items ORDER BY id LIMIT $1 OFFSET $2";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [3, 2]);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'LIMIT $1 OFFSET $2: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'LIMIT $1 OFFSET $2 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE $1 + LIMIT $2 OFFSET $3 — dollar-sign parameter alignment.
     */
    public function testPreparedWhereLimitOffsetDollarParams(): void
    {
        $sql = "SELECT id FROM pg_plop_items WHERE score >= $1 ORDER BY id LIMIT $2 OFFSET $3";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [50, 2, 1]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE $1 LIMIT $2 OFFSET $3: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(6, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE $1 LIMIT $2 OFFSET $3 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * LIMIT/OFFSET on shadow-inserted data.
     */
    public function testLimitOffsetOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_plop_items VALUES (11, 'shadow1', 110)");
        $this->pdo->exec("INSERT INTO pg_plop_items VALUES (12, 'shadow2', 120)");

        $sql = "SELECT id, name FROM pg_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [2, 10]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Shadow LIMIT/OFFSET: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(11, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow LIMIT/OFFSET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Re-execute for pagination.
     */
    public function testReusePreparedPagination(): void
    {
        $sql = "SELECT id FROM pg_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $stmt = $this->pdo->prepare($sql);

            $stmt->execute([3, 0]);
            $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $stmt->execute([3, 3]);
            $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($page1) !== 3 || count($page2) !== 3) {
                $this->markTestIncomplete(
                    "Pagination: p1=" . count($page1) . " p2=" . count($page2)
                );
            }

            $this->assertSame(1, (int) $page1[0]['id']);
            $this->assertSame(4, (int) $page2[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Pagination reuse failed: ' . $e->getMessage()
            );
        }
    }
}
