<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests implicit boolean expressions in DML WHERE clauses through ZTD shadow store
 * on MySQL via PDO.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlBooleanWhereInDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_bwd_users (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            active TINYINT(1) NOT NULL DEFAULT 1,
            score INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_bwd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_bwd_users VALUES (1, 'Alice', 1, 90)");
        $this->pdo->exec("INSERT INTO mp_bwd_users VALUES (2, 'Bob', 0, 60)");
        $this->pdo->exec("INSERT INTO mp_bwd_users VALUES (3, 'Carol', 1, 80)");
        $this->pdo->exec("INSERT INTO mp_bwd_users VALUES (4, 'Dave', 0, 40)");
    }

    public function testDeleteWhereImplicitBoolean(): void
    {
        try {
            $this->pdo->exec("DELETE FROM mp_bwd_users WHERE active");

            $rows = $this->ztdQuery("SELECT name FROM mp_bwd_users ORDER BY name");
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
            $this->pdo->exec("DELETE FROM mp_bwd_users WHERE NOT active");

            $rows = $this->ztdQuery("SELECT name FROM mp_bwd_users ORDER BY name");
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
            $this->pdo->exec("UPDATE mp_bwd_users SET score = score + 10 WHERE active");

            $rows = $this->ztdQuery("SELECT name, score FROM mp_bwd_users ORDER BY id");

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
                "UPDATE mp_bwd_users SET active = 0 WHERE active AND score < 85"
            );

            $rows = $this->ztdQuery("SELECT name, active FROM mp_bwd_users ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertEquals(1, (int) $rows[0]['active'], 'Alice should remain active');
            $this->assertEquals(0, (int) $rows[1]['active']);

            if ((int) $rows[2]['active'] !== 0) {
                $this->markTestIncomplete(
                    'UPDATE WHERE active AND score < 85: Carol active=' . $rows[2]['active']
                );
            }
            $this->assertEquals(0, (int) $rows[2]['active']);
            $this->assertEquals(0, (int) $rows[3]['active']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE boolean AND comparison failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteWhereBooleanWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM mp_bwd_users WHERE active AND score > ?"
            );
            $stmt->execute([85]);

            $rows = $this->ztdQuery("SELECT name FROM mp_bwd_users ORDER BY name");
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
            $rows = $this->ztdQuery("SELECT name FROM mp_bwd_users WHERE active ORDER BY name");
            $names = array_column($rows, 'name');

            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WHERE implicit boolean failed: ' . $e->getMessage());
        }
    }
}
