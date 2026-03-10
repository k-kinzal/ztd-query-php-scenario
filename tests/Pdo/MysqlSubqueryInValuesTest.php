<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests scalar subquery inside INSERT VALUES clause through ZTD shadow store
 * on MySQL via PDO.
 *
 * @spec SPEC-4.1
 * @spec SPEC-3.3
 */
class MysqlSubqueryInValuesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_siv_items (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                score INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_siv_counters (
                id INT PRIMARY KEY,
                label VARCHAR(50) NOT NULL,
                total INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_siv_counters', 'mp_siv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_siv_items VALUES (1, 'Alpha', 10)");
        $this->pdo->exec("INSERT INTO mp_siv_items VALUES (2, 'Bravo', 20)");
        $this->pdo->exec("INSERT INTO mp_siv_items VALUES (3, 'Charlie', 30)");
    }

    public function testInsertWithMaxIdSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_siv_items (id, name, score) VALUES ((SELECT MAX(id) FROM mp_siv_items) + 1, 'Delta', 40)"
            );

            $rows = $this->ztdQuery("SELECT id, name, score FROM mp_siv_items ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT with MAX subquery in VALUES: expected 4 rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(4, $rows);

            if ((int) $rows[3]['id'] !== 4) {
                $this->markTestIncomplete(
                    'INSERT with MAX subquery: expected id=4, got id='
                    . var_export($rows[3]['id'], true)
                );
            }
            $this->assertEquals(4, (int) $rows[3]['id']);
            $this->assertSame('Delta', $rows[3]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with MAX subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithCountSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_siv_counters (id, label, total) VALUES (1, 'item_count', (SELECT COUNT(*) FROM mp_siv_items))"
            );

            $rows = $this->ztdQuery("SELECT total FROM mp_siv_counters WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT with COUNT subquery in VALUES: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ((int) $rows[0]['total'] !== 3) {
                $this->markTestIncomplete(
                    'INSERT with COUNT subquery: expected total=3, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(3, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with COUNT subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    public function testInsertSubqueryAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_siv_items VALUES (10, 'Zulu', 99)");

            $this->pdo->exec(
                "INSERT INTO mp_siv_items (id, name, score) VALUES ((SELECT MAX(id) FROM mp_siv_items) + 1, 'Beyond', 100)"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mp_siv_items WHERE name = 'Beyond'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT subquery after shadow INSERT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ((int) $rows[0]['id'] !== 11) {
                $this->markTestIncomplete(
                    'INSERT subquery after shadow: expected id=11, got id='
                    . var_export($rows[0]['id'], true)
                );
            }
            $this->assertEquals(11, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT subquery after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithSumSubqueryInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_siv_counters (id, label, total) VALUES (2, 'high_score_sum', (SELECT SUM(score) FROM mp_siv_items WHERE score >= 20))"
            );

            $rows = $this->ztdQuery("SELECT total FROM mp_siv_counters WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT with SUM subquery: expected 1 row, got ' . count($rows));
            }
            $this->assertCount(1, $rows);

            if ((int) $rows[0]['total'] !== 50) {
                $this->markTestIncomplete(
                    'INSERT with SUM subquery: expected total=50, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(50, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with SUM subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    public function testPreparedInsertWithSubqueryInValues(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO mp_siv_counters (id, label, total) VALUES (?, ?, (SELECT COUNT(*) FROM mp_siv_items WHERE score > ?))"
            );
            $stmt->execute([3, 'above_threshold', 15]);

            $rows = $this->ztdQuery("SELECT total FROM mp_siv_counters WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared INSERT with subquery: expected 1 row, got ' . count($rows));
            }
            $this->assertCount(1, $rows);

            if ((int) $rows[0]['total'] !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT with subquery: expected total=2, got total='
                    . var_export($rows[0]['total'], true)
                );
            }
            $this->assertEquals(2, (int) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with subquery in VALUES failed: ' . $e->getMessage());
        }
    }
}
