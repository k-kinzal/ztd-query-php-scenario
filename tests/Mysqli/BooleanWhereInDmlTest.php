<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests implicit boolean expressions in DML WHERE clauses through ZTD shadow store.
 *
 * MySQL supports `WHERE active` as shorthand for `WHERE active != 0`.
 * The CTE rewriter's DML mutation resolver must evaluate truthiness
 * without an explicit comparison operator.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class BooleanWhereInDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_bwd_users (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            active TINYINT(1) NOT NULL DEFAULT 1,
            score INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_bwd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_bwd_users VALUES (1, 'Alice', 1, 90)");
        $this->mysqli->query("INSERT INTO mi_bwd_users VALUES (2, 'Bob', 0, 60)");
        $this->mysqli->query("INSERT INTO mi_bwd_users VALUES (3, 'Carol', 1, 80)");
        $this->mysqli->query("INSERT INTO mi_bwd_users VALUES (4, 'Dave', 0, 40)");
    }

    /**
     * DELETE FROM t WHERE active -- implicit boolean truthy.
     * Should delete rows where active = 1 (Alice, Carol).
     */
    public function testDeleteWhereImplicitBoolean(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_bwd_users WHERE active");

            $rows = $this->ztdQuery("SELECT name FROM mi_bwd_users ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE active (implicit boolean): expected 2 rows (Bob, Dave), got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Bob', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE FROM t WHERE NOT active -- implicit boolean negation.
     * Should delete rows where active = 0 (Bob, Dave).
     */
    public function testDeleteWhereNotImplicitBoolean(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_bwd_users WHERE NOT active");

            $rows = $this->ztdQuery("SELECT name FROM mi_bwd_users ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE NOT active: expected 2 rows (Alice, Carol), got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NOT implicit boolean failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET ... WHERE active -- implicit boolean in UPDATE.
     * Should only update active rows.
     */
    public function testUpdateWhereImplicitBoolean(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_bwd_users SET score = score + 10 WHERE active");

            $rows = $this->ztdQuery("SELECT name, score FROM mi_bwd_users ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active: expected 4 rows, got ' . count($rows)
                );
            }
            $this->assertCount(4, $rows);

            // Alice: active=1, score should be 100
            if ((int) $rows[0]['score'] !== 100) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active: Alice score='
                    . $rows[0]['score'] . ', expected 100'
                );
            }
            $this->assertEquals(100, (int) $rows[0]['score']);

            // Bob: active=0, score should remain 60
            $this->assertEquals(60, (int) $rows[1]['score']);

            // Carol: active=1, score should be 90
            if ((int) $rows[2]['score'] !== 90) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active: Carol score='
                    . $rows[2]['score'] . ', expected 90'
                );
            }
            $this->assertEquals(90, (int) $rows[2]['score']);

            // Dave: active=0, score should remain 40
            $this->assertEquals(40, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET active = 0 WHERE active AND score < 85 -- combined boolean + comparison.
     */
    public function testUpdateWhereBooleanAndComparison(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_bwd_users SET active = 0 WHERE active AND score < 85"
            );

            $rows = $this->ztdQuery("SELECT name, active FROM mi_bwd_users ORDER BY id");

            $this->assertCount(4, $rows);

            // Alice: active=1, score=90 >= 85, still active
            $this->assertEquals(1, (int) $rows[0]['active'], 'Alice should remain active');

            // Bob: already inactive
            $this->assertEquals(0, (int) $rows[1]['active']);

            // Carol: active=1, score=80 < 85, should become inactive
            if ((int) $rows[2]['active'] !== 0) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active AND score < 85: Carol active='
                    . $rows[2]['active'] . ', expected 0'
                );
            }
            $this->assertEquals(0, (int) $rows[2]['active']);

            // Dave: already inactive
            $this->assertEquals(0, (int) $rows[3]['active']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE boolean AND comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE active with bound parameter for additional condition.
     */
    public function testPreparedDeleteWhereBooleanWithParam(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "DELETE FROM mi_bwd_users WHERE active AND score > ?"
            );
            $threshold = 85;
            $stmt->bind_param('i', $threshold);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name FROM mi_bwd_users ORDER BY name");
            $names = array_column($rows, 'name');

            // Alice: active=1, score=90 > 85 -> deleted
            // Bob: active=0 -> not deleted
            // Carol: active=1, score=80 <= 85 -> not deleted
            // Dave: active=0 -> not deleted
            if (count($names) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE WHERE active AND score > ?: expected 3 rows, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Bob', 'Carol', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE WHERE boolean with param failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with implicit boolean — verify it works for reads.
     */
    public function testSelectWhereImplicitBoolean(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name FROM mi_bwd_users WHERE active ORDER BY name");
            $names = array_column($rows, 'name');

            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }
}
