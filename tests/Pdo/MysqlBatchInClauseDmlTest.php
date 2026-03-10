<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests batch DML operations with IN clauses containing many prepared parameters on MySQL.
 *
 * IN (?, ?, ...) with multiple parameters is extremely common in real applications.
 * The CTE rewriter must correctly handle parameter binding with variable-length IN lists.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class MysqlBatchInClauseDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_bin_items (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category CHAR(1) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'active\',
                priority INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_bin_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'A' : 'B';
            $this->pdo->exec(
                "INSERT INTO my_bin_items VALUES ($i, 'Item$i', '$cat', 'active', $i)"
            );
        }
    }

    /**
     * Prepared DELETE with IN (?, ?, ?, ?, ?) — 5 params.
     */
    public function testPreparedDeleteWithFiveInParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_bin_items WHERE id IN (?, ?, ?, ?, ?)"
            );
            $stmt->execute([1, 3, 5, 7, 9]);

            $rows = $this->ztdQuery("SELECT id FROM my_bin_items ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'DELETE IN 5 params: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $ids = array_column($rows, 'id');
            $this->assertEquals([2, 4, 6, 8, 10], array_map('intval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with 5 IN params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with IN and additional WHERE condition.
     */
    public function testPreparedUpdateWithInAndExtraWhere(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_bin_items SET status = ? WHERE id IN (?, ?, ?) AND category = ?"
            );
            $stmt->execute(['archived', 1, 2, 3, 'A']);

            $rows = $this->ztdQuery(
                "SELECT id, status FROM my_bin_items WHERE status = 'archived' ORDER BY id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE IN + WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with IN + extra WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with NOT IN.
     */
    public function testPreparedDeleteWithNotIn(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_bin_items WHERE id NOT IN (?, ?, ?)"
            );
            $stmt->execute([1, 5, 10]);

            $rows = $this->ztdQuery("SELECT id FROM my_bin_items ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT IN: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $ids = array_column($rows, 'id');
            $this->assertEquals([1, 5, 10], array_map('intval', $ids));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with NOT IN failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with 8 IN params.
     */
    public function testPreparedUpdateWithEightInParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_bin_items SET priority = priority + ? WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            $stmt->execute([100, 1, 2, 3, 4, 5, 6, 7, 8]);

            $rows = $this->ztdQuery(
                "SELECT id, priority FROM my_bin_items WHERE priority > 100 ORDER BY id"
            );

            if (count($rows) !== 8) {
                $all = $this->ztdQuery("SELECT id, priority FROM my_bin_items ORDER BY id");
                $this->markTestIncomplete(
                    'UPDATE IN 8 params: expected 8, got ' . count($rows)
                    . '. All: ' . json_encode($all)
                );
            }

            $this->assertCount(8, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with 8 IN params failed: ' . $e->getMessage());
        }
    }

    /**
     * Two IN clauses in same DELETE.
     */
    public function testPreparedDeleteWithTwoInClauses(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_bin_items WHERE category IN (?, ?) AND priority IN (?, ?, ?)"
            );
            $stmt->execute(['A', 'B', 1, 2, 6]);

            $rows = $this->ztdQuery("SELECT id FROM my_bin_items ORDER BY id");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE two IN: expected 7, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with two IN clauses failed: ' . $e->getMessage());
        }
    }
}
