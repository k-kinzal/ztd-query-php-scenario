<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CTE MATERIALIZED/NOT MATERIALIZED hints on PostgreSQL.
 *
 * PostgreSQL 12+ supports MATERIALIZED and NOT MATERIALIZED hints
 * on WITH clauses to control CTE optimization.
 *
 * Since ZTD rewrites user CTEs with its own shadow CTE, these hints
 * are overwritten — the user CTE body reads from the physical table
 * (empty) rather than shadow data. This is consistent with SPEC-11.PG-CTE.
 *
 * @spec SPEC-10.2.28
 * @see SPEC-11.PG-CTE
 */
class PostgresCteMaterializedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ctem_test (id INT PRIMARY KEY, name VARCHAR(50), active INT DEFAULT 1)';
    }

    protected function getTableNames(): array
    {
        return ['pg_ctem_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ctem_test VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_ctem_test VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_ctem_test VALUES (3, 'Charlie', 0)");
    }

    /**
     * WITH ... AS MATERIALIZED — user CTE is overwritten by ZTD shadow CTE.
     *
     * The user CTE inner query reads from the physical table (empty on PostgreSQL),
     * so the result is 0 rows. This is consistent with SPEC-11.PG-CTE.
     */
    public function testCteMaterializedHintOverwritten(): void
    {
        $stmt = $this->pdo->query(
            'WITH active_users AS MATERIALIZED (
                SELECT name FROM pg_ctem_test WHERE active = 1
            )
            SELECT * FROM active_users ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // ZTD replaces the WITH clause — the user CTE reads from physical table → 0 rows
        $this->assertCount(0, $rows, 'User CTE with MATERIALIZED hint returns empty (SPEC-11.PG-CTE)');
    }

    /**
     * WITH ... AS NOT MATERIALIZED — same behavior: ZTD replaces the WITH clause.
     */
    public function testCteNotMaterializedHintOverwritten(): void
    {
        $stmt = $this->pdo->query(
            'WITH inactive AS NOT MATERIALIZED (
                SELECT name FROM pg_ctem_test WHERE active = 0
            )
            SELECT * FROM inactive'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // ZTD replaces the WITH clause — user CTE reads from physical table → 0 rows
        $this->assertCount(0, $rows, 'User CTE with NOT MATERIALIZED hint returns empty (SPEC-11.PG-CTE)');
    }

    /**
     * Regular query without CTE hints works normally.
     */
    public function testRegularSelectWorks(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pg_ctem_test WHERE active = 1 ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob'], $rows);
    }

    /**
     * Shadow mutations visible in regular queries.
     */
    public function testShadowMutationVisible(): void
    {
        $this->pdo->exec("INSERT INTO pg_ctem_test VALUES (4, 'Diana', 1)");
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctem_test WHERE active = 1');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ctem_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
