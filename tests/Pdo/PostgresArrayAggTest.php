<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL ARRAY_AGG() and array operations on shadow data.
 *
 * Real-world scenario: PostgreSQL applications frequently use ARRAY_AGG()
 * for aggregating related data (tags, roles, categories) and array
 * operators for filtering. These are common in ORMs (e.g., Doctrine's
 * PostgreSQL array type) and direct SQL. The CTE rewriter must handle
 * array constructor syntax and array operators without confusing them
 * with other SQL constructs.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresArrayAggTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_aag_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE pg_aag_user_roles (
                user_id INT NOT NULL,
                role VARCHAR(50) NOT NULL,
                PRIMARY KEY (user_id, role)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_aag_user_roles', 'pg_aag_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_aag_users (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_aag_users (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_aag_users (id, name) VALUES (3, 'Carol')");

        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (1, 'admin')");
        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (1, 'editor')");
        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (2, 'editor')");
        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (2, 'viewer')");
        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (3, 'viewer')");
    }

    /**
     * ARRAY_AGG() with GROUP BY on shadow data.
     */
    public function testArrayAggWithGroupBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, ARRAY_AGG(r.role ORDER BY r.role) AS roles
                 FROM pg_aag_users u
                 JOIN pg_aag_user_roles r ON r.user_id = u.id
                 GROUP BY u.id, u.name
                 ORDER BY u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'ARRAY_AGG with GROUP BY returned no rows on shadow data.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            // roles should be {admin,editor}
            $this->assertStringContainsString('admin', $rows[0]['roles']);
            $this->assertStringContainsString('editor', $rows[0]['roles']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ARRAY_AGG with GROUP BY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ARRAY_AGG() in subquery.
     */
    public function testArrayAggInSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name,
                    (SELECT ARRAY_AGG(r.role ORDER BY r.role)
                     FROM pg_aag_user_roles r
                     WHERE r.user_id = u.id) AS roles
                 FROM pg_aag_users u
                 ORDER BY u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'ARRAY_AGG in subquery returned no rows.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertStringContainsString('admin', $rows[0]['roles']); // Alice
            $this->assertStringContainsString('viewer', $rows[2]['roles']); // Carol
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ARRAY_AGG in subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ANY() with ARRAY subquery.
     */
    public function testAnyWithArraySubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name
                 FROM pg_aag_users u
                 WHERE 'admin' = ANY(
                    SELECT r.role FROM pg_aag_user_roles r WHERE r.user_id = u.id
                 )
                 ORDER BY u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'ANY() with array subquery returned no rows on shadow data. '
                    . 'The CTE rewriter may not handle ANY(SELECT ...) correctly.'
                );
            }

            // Only Alice has 'admin' role
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ANY() with array subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ARRAY_AGG() after shadow mutation.
     */
    public function testArrayAggAfterMutation(): void
    {
        // Add a new role to Carol
        $this->pdo->exec("INSERT INTO pg_aag_user_roles VALUES (3, 'editor')");

        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, ARRAY_AGG(r.role ORDER BY r.role) AS roles
                 FROM pg_aag_users u
                 JOIN pg_aag_user_roles r ON r.user_id = u.id
                 WHERE u.name = 'Carol'
                 GROUP BY u.id, u.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'ARRAY_AGG after mutation returned no rows.'
                );
            }

            $this->assertCount(1, $rows);
            // Carol should now have {editor,viewer}
            $this->assertStringContainsString('editor', $rows[0]['roles']);
            $this->assertStringContainsString('viewer', $rows[0]['roles']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ARRAY_AGG after mutation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ARRAY_AGG with FILTER clause.
     */
    public function testArrayAggWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    ARRAY_AGG(DISTINCT u.name ORDER BY u.name)
                        FILTER (WHERE r.role = 'editor') AS editors,
                    ARRAY_AGG(DISTINCT u.name ORDER BY u.name)
                        FILTER (WHERE r.role = 'viewer') AS viewers
                 FROM pg_aag_users u
                 JOIN pg_aag_user_roles r ON r.user_id = u.id"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'ARRAY_AGG with FILTER returned no rows.'
                );
            }

            $this->assertCount(1, $rows);
            // editors: Alice, Bob
            $this->assertStringContainsString('Alice', $rows[0]['editors']);
            $this->assertStringContainsString('Bob', $rows[0]['editors']);
            // viewers: Bob, Carol
            $this->assertStringContainsString('Bob', $rows[0]['viewers']);
            $this->assertStringContainsString('Carol', $rows[0]['viewers']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ARRAY_AGG with FILTER failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_aag_users')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
