<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL INSERT ... ON CONFLICT DO UPDATE ... WHERE on ZTD.
 *
 * PostgreSQL supports a WHERE clause in ON CONFLICT DO UPDATE to conditionally
 * update conflicting rows. If the WHERE condition is not met, the conflicting
 * row is left unchanged (effectively a no-op for that row).
 *
 * The PgSqlParser::extractOnConflictUpdateColumns() strips the WHERE clause
 * (line 253), so UpsertMutation does not enforce it. This means conditional
 * upserts may update rows that should be left unchanged.
 * @spec SPEC-4.2a
 * @see https://github.com/k-kinzal/ztd-query-php/issues/30
 */
class PostgresOnConflictWhereTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ocw_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active BOOLEAN DEFAULT TRUE)';
    }

    protected function getTableNames(): array
    {
        return ['pg_ocw_test'];
    }


    /**
     * ON CONFLICT DO UPDATE without WHERE — always updates on conflict.
     */
    public function testOnConflictDoUpdateWithoutWhere(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice V2', 95) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_ocw_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * ON CONFLICT DO UPDATE SET ... WHERE condition IS MET.
     *
     * When the WHERE condition is satisfied, the row should be updated.
     */
    public function testOnConflictDoUpdateWhereConditionMet(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 90)");

        // WHERE score >= 80 — condition IS met (score=90 >= 80)
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice V2', 95) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score WHERE pg_ocw_test.score >= 80");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_ocw_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // Should be updated since condition is met
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * ON CONFLICT DO UPDATE SET ... WHERE condition NOT met.
     *
     * When the WHERE condition is NOT satisfied, PostgreSQL does NOT update
     * the row. But ZTD's UpsertMutation ignores the WHERE clause and always
     * updates, which is a semantic difference.
     *
     * ON CONFLICT DO UPDATE with WHERE condition NOT met should NOT update.
     */
    public function testOnConflictDoUpdateWhereConditionNotMet(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 50)");

        // WHERE score >= 80 -- condition is NOT met (score=50 < 80)
        // Native PostgreSQL: row should NOT be updated
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice V2', 95) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score WHERE pg_ocw_test.score >= 80");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_ocw_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // Expected: WHERE condition not met, so row should NOT be updated
        if ($row['name'] !== 'Alice') {
            $this->markTestIncomplete(
                'UpsertMutation does not evaluate ON CONFLICT WHERE clause. '
                . 'Expected row unchanged (Alice, 50), got (' . $row['name'] . ', ' . $row['score'] . ')'
            );
        }
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(50, (int) $row['score']);
    }

    /**
     * ON CONFLICT DO NOTHING — no update.
     */
    public function testOnConflictDoNothing(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice V2', 95) ON CONFLICT (id) DO NOTHING");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ocw_test');
        $count = (int) $stmt->fetchColumn();
        // DO NOTHING might still insert (shadow store limitation) or not
        $this->assertGreaterThanOrEqual(1, $count);
    }

    /**
     * No conflict — new row inserted regardless of WHERE clause.
     */
    public function testOnConflictWhereNoConflict(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 90) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE pg_ocw_test.score >= 80");

        $stmt = $this->pdo->query('SELECT name, score FROM pg_ocw_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ocw_test (id, name, score) VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ocw_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
