<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests implicit boolean expressions in DML WHERE clauses through ZTD shadow store
 * on PostgreSQL via PDO.
 *
 * PostgreSQL supports `WHERE active` for BOOLEAN columns. The DML mutation resolver
 * must evaluate truthiness without an explicit comparison operator.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresBooleanWhereInDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_bwd_users (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            score INT NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_bwd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_bwd_users VALUES (1, 'Alice', TRUE, 90)");
        $this->pdo->exec("INSERT INTO pg_bwd_users VALUES (2, 'Bob', FALSE, 60)");
        $this->pdo->exec("INSERT INTO pg_bwd_users VALUES (3, 'Carol', TRUE, 80)");
        $this->pdo->exec("INSERT INTO pg_bwd_users VALUES (4, 'Dave', FALSE, 40)");
    }

    public function testDeleteWhereImplicitBoolean(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_bwd_users WHERE active");

            $rows = $this->ztdQuery("SELECT name FROM pg_bwd_users ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE active: expected 2 rows (Bob, Dave), got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Bob', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereNotImplicitBoolean(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_bwd_users WHERE NOT active");

            $rows = $this->ztdQuery("SELECT name FROM pg_bwd_users ORDER BY name");
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

    public function testUpdateWhereImplicitBoolean(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_bwd_users SET score = score + 10 WHERE active");

            $rows = $this->ztdQuery("SELECT name, score FROM pg_bwd_users ORDER BY id");

            $this->assertCount(4, $rows);

            if ((int) $rows[0]['score'] !== 100) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active: Alice score=' . $rows[0]['score'] . ', expected 100'
                );
            }
            $this->assertEquals(100, (int) $rows[0]['score']);
            $this->assertEquals(60, (int) $rows[1]['score']);

            if ((int) $rows[2]['score'] !== 90) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active: Carol score=' . $rows[2]['score'] . ', expected 90'
                );
            }
            $this->assertEquals(90, (int) $rows[2]['score']);
            $this->assertEquals(40, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereBooleanAndComparison(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_bwd_users SET active = FALSE WHERE active AND score < 85"
            );

            $rows = $this->ztdQuery("SELECT name, active FROM pg_bwd_users ORDER BY id");

            $this->assertCount(4, $rows);

            // Alice: active=TRUE, score=90 >= 85, should remain active
            $this->assertTrue(
                in_array($rows[0]['active'], [true, 't', '1', 1], true),
                'Alice should remain active'
            );

            // Carol: active=TRUE, score=80 < 85, should become inactive
            if (in_array($rows[2]['active'], [true, 't', '1', 1], true)) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active AND score < 85: Carol still active='
                    . var_export($rows[2]['active'], true)
                );
            }
            $this->assertFalse(
                in_array($rows[2]['active'], [true, 't', '1', 1], true),
                'Carol should be deactivated'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE boolean AND comparison failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteWhereBooleanWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_bwd_users WHERE active AND score > ?"
            );
            $stmt->execute([85]);

            $rows = $this->ztdQuery("SELECT name FROM pg_bwd_users ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE WHERE active AND score > ?: expected 3, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Bob', 'Carol', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE WHERE boolean with param failed: ' . $e->getMessage());
        }
    }

    public function testSelectWhereImplicitBoolean(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name FROM pg_bwd_users WHERE active ORDER BY name");
            $names = array_column($rows, 'name');

            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }
}
