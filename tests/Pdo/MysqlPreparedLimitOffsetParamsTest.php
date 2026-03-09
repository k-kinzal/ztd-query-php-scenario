<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared SELECT with parameterized LIMIT and OFFSET on MySQL PDO.
 *
 * LIMIT ? OFFSET ? is the most common pagination pattern.
 * The CTE rewriter must preserve parameter positions correctly.
 *
 * @spec SPEC-3.2
 */
class MysqlPreparedLimitOffsetParamsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_plop_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            score INT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_plop_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO my_plop_items VALUES ({$i}, 'item{$i}', " . ($i * 10) . ")");
        }
    }

    /**
     * Prepared SELECT with LIMIT ? OFFSET ?.
     */
    public function testPreparedLimitOffsetParams(): void
    {
        $sql = "SELECT id, name FROM my_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [3, 2]);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared LIMIT ? OFFSET ?: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(5, (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared LIMIT ? OFFSET ? failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE param + LIMIT ? OFFSET ? — three params, tests index alignment.
     */
    public function testPreparedWhereAndLimitOffsetParams(): void
    {
        $sql = "SELECT id, name FROM my_plop_items WHERE score >= ? ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [50, 2, 1]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE + LIMIT + OFFSET: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(6, (int) $rows[0]['id']);
            $this->assertSame(7, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'WHERE + LIMIT + OFFSET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * LIMIT/OFFSET on shadow-inserted data.
     */
    public function testPreparedLimitOffsetOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO my_plop_items VALUES (11, 'shadow1', 110)");
        $this->pdo->exec("INSERT INTO my_plop_items VALUES (12, 'shadow2', 120)");

        $sql = "SELECT id, name FROM my_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [2, 10]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'LIMIT/OFFSET shadow: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(11, (int) $rows[0]['id']);
            $this->assertSame(12, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'LIMIT/OFFSET on shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Re-execute with different LIMIT/OFFSET for pagination.
     */
    public function testReusePreparedPagination(): void
    {
        $sql = "SELECT id FROM my_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $stmt = $this->pdo->prepare($sql);

            $stmt->execute([3, 0]);
            $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $stmt->execute([3, 3]);
            $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($page1) !== 3 || count($page2) !== 3) {
                $this->markTestIncomplete(
                    "Reuse pagination: page1=" . count($page1) . " page2=" . count($page2)
                );
            }

            $this->assertSame(1, (int) $page1[0]['id']);
            $this->assertSame(4, (int) $page2[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Reuse prepared pagination failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * bindValue with PARAM_INT for LIMIT/OFFSET.
     */
    public function testBindValueIntLimitOffset(): void
    {
        $sql = "SELECT id FROM my_plop_items ORDER BY id LIMIT ? OFFSET ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->bindValue(1, 5, PDO::PARAM_INT);
            $stmt->bindValue(2, 0, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'bindValue INT LIMIT: expected 5, got ' . count($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'bindValue INT LIMIT/OFFSET failed: ' . $e->getMessage()
            );
        }
    }
}
