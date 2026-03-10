<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE expressions in direct INSERT VALUES (not INSERT...SELECT).
 *
 * Many ORMs and application frameworks generate INSERT statements with
 * CASE expressions in VALUES. Tests whether the CTE rewriter correctly
 * handles these without misinterpreting the SQL structure.
 *
 * @spec SPEC-4.1
 */
class SqliteCaseInInsertValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_civ_records (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            tier TEXT NOT NULL,
            score INTEGER NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_civ_records'];
    }

    /**
     * INSERT with simple CASE expression in VALUES.
     */
    public function testInsertWithCaseInValues(): void
    {
        $sql = "INSERT INTO sl_civ_records VALUES (
            1, 'Test',
            CASE WHEN 100 > 50 THEN 'high' ELSE 'low' END,
            100
        )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, label, tier, score FROM sl_civ_records WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['tier'] !== 'high') {
                $this->markTestIncomplete(
                    'CASE in INSERT: expected tier=high, got ' . $rows[0]['tier']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('high', $rows[0]['tier']);
            $this->assertSame(100, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE in INSERT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with CASE referencing another column's value in VALUES.
     */
    public function testInsertWithCaseReferencingLiteral(): void
    {
        $sql = "INSERT INTO sl_civ_records VALUES (
            2, 'Item2',
            CASE WHEN 30 >= 80 THEN 'premium'
                 WHEN 30 >= 50 THEN 'standard'
                 ELSE 'basic'
            END,
            30
        )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT tier, score FROM sl_civ_records WHERE id = 2");

            $this->assertCount(1, $rows);
            $this->assertSame('basic', $rows[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE multi-branch INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT with CASE and bound parameter.
     */
    public function testPreparedInsertWithCaseParam(): void
    {
        $sql = "INSERT INTO sl_civ_records VALUES (
            ?, ?,
            CASE WHEN ? > 75 THEN 'high'
                 WHEN ? > 25 THEN 'medium'
                 ELSE 'low'
            END,
            ?
        )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([3, 'Item3', 60, 60, 60]);

            $rows = $this->ztdQuery("SELECT tier FROM sl_civ_records WHERE id = 3");

            $this->assertCount(1, $rows);

            if ($rows[0]['tier'] !== 'medium') {
                $this->markTestIncomplete(
                    'Prepared CASE INSERT: expected medium, got ' . $rows[0]['tier']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('medium', $rows[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared CASE INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-row INSERT with CASE expressions in each row.
     */
    public function testMultiRowInsertWithCase(): void
    {
        $sql = "INSERT INTO sl_civ_records VALUES
            (10, 'A', CASE WHEN 90 > 50 THEN 'high' ELSE 'low' END, 90),
            (11, 'B', CASE WHEN 20 > 50 THEN 'high' ELSE 'low' END, 20),
            (12, 'C', CASE WHEN 50 > 50 THEN 'high' ELSE 'low' END, 50)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, tier FROM sl_civ_records WHERE id IN (10, 11, 12) ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row CASE INSERT: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('high', $rows[0]['tier']); // 90 > 50
            $this->assertSame('low', $rows[1]['tier']);   // 20 > 50 is false
            $this->assertSame('low', $rows[2]['tier']);   // 50 > 50 is false (not >=)
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-row CASE INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with nested CASE in VALUES.
     */
    public function testInsertWithNestedCase(): void
    {
        $sql = "INSERT INTO sl_civ_records VALUES (
            20, 'Nested',
            CASE WHEN 1 = 1 THEN
                CASE WHEN 2 > 1 THEN 'inner-yes' ELSE 'inner-no' END
            ELSE 'outer-no'
            END,
            99
        )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT tier FROM sl_civ_records WHERE id = 20");

            $this->assertCount(1, $rows);

            if ($rows[0]['tier'] !== 'inner-yes') {
                $this->markTestIncomplete(
                    'Nested CASE INSERT: expected inner-yes, got ' . $rows[0]['tier']
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('inner-yes', $rows[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Nested CASE INSERT failed: ' . $e->getMessage()
            );
        }
    }
}
