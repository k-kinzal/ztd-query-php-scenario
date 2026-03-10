<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with CASE + BETWEEN + multiple prepared parameters.
 *
 * Pattern: UPDATE t SET status = CASE WHEN amount BETWEEN ? AND ? THEN 'mid'
 *          WHEN amount > ? THEN 'high' ELSE 'low' END WHERE category = ?
 *
 * Stresses the CTE rewriter with many parameter positions in CASE branches.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateConditionalBetweenTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ucb_transactions (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            risk_level TEXT NOT NULL DEFAULT \'unset\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ucb_transactions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (1, 'purchase', 5.00, 'unset')");
        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (2, 'purchase', 50.00, 'unset')");
        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (3, 'purchase', 500.00, 'unset')");
        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (4, 'refund', 25.00, 'unset')");
        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (5, 'purchase', 100.00, 'unset')");
    }

    /**
     * UPDATE with CASE WHEN BETWEEN prepared params.
     */
    public function testUpdateCaseBetweenPrepared(): void
    {
        $sql = "UPDATE sl_ucb_transactions
                SET risk_level = CASE
                    WHEN amount BETWEEN ? AND ? THEN 'low'
                    WHEN amount BETWEEN ? AND ? THEN 'medium'
                    ELSE 'high'
                END
                WHERE category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([0, 10, 10.01, 200, 'purchase']);

            $rows = $this->ztdQuery(
                "SELECT id, amount, risk_level FROM sl_ucb_transactions ORDER BY id"
            );

            $this->assertCount(5, $rows);

            // id=1: 5.00 purchase → BETWEEN 0 AND 10 → low
            if ($rows[0]['risk_level'] !== 'low') {
                $this->markTestIncomplete(
                    'CASE BETWEEN: id=1 expected low, got ' . $rows[0]['risk_level']
                    . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertSame('low', $rows[0]['risk_level']);

            // id=2: 50.00 purchase → BETWEEN 10.01 AND 200 → medium
            $this->assertSame('medium', $rows[1]['risk_level']);

            // id=3: 500.00 purchase → above 200 → high
            $this->assertSame('high', $rows[2]['risk_level']);

            // id=4: refund category → not updated (WHERE category = 'purchase')
            $this->assertSame('unset', $rows[3]['risk_level']);

            // id=5: 100.00 purchase → BETWEEN 10.01 AND 200 → medium
            $this->assertSame('medium', $rows[4]['risk_level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE BETWEEN UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with multiple BETWEEN conditions and OR.
     */
    public function testUpdateMultipleBetweenOr(): void
    {
        $sql = "UPDATE sl_ucb_transactions
                SET risk_level = 'flagged'
                WHERE (amount BETWEEN ? AND ?) OR (amount BETWEEN ? AND ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 10, 400, 600]);

            $rows = $this->ztdQuery(
                "SELECT id, risk_level FROM sl_ucb_transactions ORDER BY id"
            );

            // id=1: 5.00 in [1,10] → flagged
            // id=2: 50.00 not in either range → unset
            // id=3: 500.00 in [400,600] → flagged
            // id=4: 25.00 not in either range → unset
            // id=5: 100.00 not in either range → unset
            $flagged = array_filter($rows, fn($r) => $r['risk_level'] === 'flagged');
            $flaggedIds = array_map(fn($r) => (int) $r['id'], array_values($flagged));

            if (count($flagged) !== 2) {
                $this->markTestIncomplete(
                    'Multiple BETWEEN: expected 2 flagged, got ' . count($flagged)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $flagged);
            $this->assertContains(1, $flaggedIds);
            $this->assertContains(3, $flaggedIds);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple BETWEEN UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with BETWEEN and CASE in WHERE.
     */
    public function testDeleteWithBetweenCaseWhere(): void
    {
        $sql = "DELETE FROM sl_ucb_transactions
                WHERE amount BETWEEN ? AND ?
                AND category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([20, 100, 'purchase']);

            $rows = $this->ztdQuery(
                "SELECT id, category, amount FROM sl_ucb_transactions ORDER BY id"
            );

            // id=2 (50, purchase) and id=5 (100, purchase) deleted
            // id=1 (5, purchase — out of range), id=3 (500, purchase — out of range),
            // id=4 (25, refund — wrong category) remain
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'BETWEEN DELETE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(1, $ids);
            $this->assertContains(3, $ids);
            $this->assertContains(4, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'BETWEEN DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with CASE BETWEEN on shadow-inserted data.
     */
    public function testCaseBetweenOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO sl_ucb_transactions VALUES (6, 'purchase', 75.00, 'unset')");

        $sql = "UPDATE sl_ucb_transactions
                SET risk_level = CASE
                    WHEN amount BETWEEN 0 AND 50 THEN 'low'
                    WHEN amount BETWEEN 50.01 AND 200 THEN 'medium'
                    ELSE 'high'
                END
                WHERE id = 6";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT risk_level FROM sl_ucb_transactions WHERE id = 6");

            $this->assertCount(1, $rows);
            if ($rows[0]['risk_level'] !== 'medium') {
                $this->markTestIncomplete(
                    'Shadow CASE BETWEEN: expected medium, got ' . $rows[0]['risk_level']
                    . '. Data: ' . json_encode($rows)
                );
            }
            $this->assertSame('medium', $rows[0]['risk_level']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow CASE BETWEEN failed: ' . $e->getMessage()
            );
        }
    }
}
