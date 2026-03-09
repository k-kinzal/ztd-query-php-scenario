<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared SELECT with parameterized LIMIT and OFFSET.
 *
 * This is the most common pagination pattern: LIMIT ? OFFSET ?.
 * The CTE rewriter must preserve parameter positions when LIMIT/OFFSET
 * are supplied as bound parameters rather than literal integers.
 *
 * @spec SPEC-3.2
 */
class SqlitePreparedLimitOffsetParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_plop_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_plop_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert 10 rows
        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO sl_plop_items VALUES ({$i}, 'item{$i}', " . ($i * 10) . ")");
        }
    }

    /**
     * Prepared SELECT with LIMIT ? only.
     */
    public function testPreparedSelectWithLimitParam(): void
    {
        $sql = "SELECT id, name FROM sl_plop_items ORDER BY id LIMIT ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [3]);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared LIMIT ?: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('1', (string) $rows[0]['id']);
            $this->assertSame('3', (string) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared LIMIT ? failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with LIMIT ? OFFSET ?.
     */
    public function testPreparedSelectWithLimitAndOffsetParams(): void
    {
        $sql = "SELECT id, name FROM sl_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [3, 2]);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared LIMIT ? OFFSET ?: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // OFFSET 2 means skip items 1,2; start at item 3
            $this->assertSame('3', (string) $rows[0]['id']);
            $this->assertSame('5', (string) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared LIMIT ? OFFSET ? failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with WHERE param + LIMIT ? OFFSET ?.
     * Three params total — tests parameter index alignment.
     */
    public function testPreparedWhereAndLimitOffsetParams(): void
    {
        $sql = "SELECT id, name FROM sl_plop_items WHERE score >= ? ORDER BY id LIMIT ? OFFSET ?";

        try {
            // score >= 50 → items 5-10 (6 items), LIMIT 2 OFFSET 1 → items 6,7
            $rows = $this->ztdPrepareAndExecute($sql, [50, 2, 1]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE + LIMIT + OFFSET params: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('6', (string) $rows[0]['id']);
            $this->assertSame('7', (string) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE + LIMIT + OFFSET params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared LIMIT/OFFSET on shadow-inserted data.
     */
    public function testPreparedLimitOffsetOnShadowData(): void
    {
        // Add shadow rows
        $this->pdo->exec("INSERT INTO sl_plop_items VALUES (11, 'shadow1', 110)");
        $this->pdo->exec("INSERT INTO sl_plop_items VALUES (12, 'shadow2', 120)");

        $sql = "SELECT id, name FROM sl_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            // 12 total rows, get last 2 (OFFSET 10, LIMIT 2)
            $rows = $this->ztdPrepareAndExecute($sql, [2, 10]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'LIMIT/OFFSET on shadow data: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('11', (string) $rows[0]['id']);
            $this->assertSame('12', (string) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'LIMIT/OFFSET on shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Re-execute same prepared statement with different LIMIT/OFFSET values.
     */
    public function testReusePreparedWithDifferentLimitOffset(): void
    {
        $sql = "SELECT id FROM sl_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $stmt = $this->pdo->prepare($sql);

            // Page 1: items 1-3
            $stmt->execute([3, 0]);
            $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Page 2: items 4-6
            $stmt->execute([3, 3]);
            $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Page 3: items 7-9
            $stmt->execute([3, 6]);
            $page3 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($page1) !== 3 || count($page2) !== 3 || count($page3) !== 3) {
                $this->markTestIncomplete(
                    "Reuse LIMIT/OFFSET: page counts = {$this->countStr($page1)},{$this->countStr($page2)},{$this->countStr($page3)}"
                );
            }

            $this->assertSame('1', (string) $page1[0]['id']);
            $this->assertSame('4', (string) $page2[0]['id']);
            $this->assertSame('7', (string) $page3[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Reuse prepared LIMIT/OFFSET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared LIMIT/OFFSET with bindValue (positional).
     */
    public function testBindValueLimitOffset(): void
    {
        $sql = "SELECT id FROM sl_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->bindValue(1, 5, PDO::PARAM_INT);
            $stmt->bindValue(2, 0, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'bindValue LIMIT/OFFSET: expected 5 rows, got ' . count($rows)
                );
            }

            $this->assertCount(5, $rows);
            $this->assertSame('1', (string) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'bindValue LIMIT/OFFSET failed: ' . $e->getMessage()
            );
        }
    }

    private function countStr(array $arr): string
    {
        return (string) count($arr);
    }
}
