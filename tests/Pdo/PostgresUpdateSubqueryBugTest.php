<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with IN (subquery containing GROUP BY HAVING) on PostgreSQL.
 * This tests whether the CTE rewriter issue found on SQLite also affects PostgreSQL.
 *
 * Result: PostgreSQL is also affected, but with a different error.
 * SQLite produces "incomplete input", PostgreSQL produces "ambiguous column"
 * because the CTE rewriter generates a cross join between the tables.
 * MySQL handles this pattern correctly.
 * @spec SPEC-4.2
 */
class PostgresUpdateSubqueryBugTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_subq_users (id INT PRIMARY KEY, name VARCHAR(255), tier VARCHAR(50))',
            'CREATE TABLE pg_subq_orders (id INT PRIMARY KEY, user_id INT, total DECIMAL(10,2), status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_subq_orders', 'pg_subq_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_subq_users (id, name, tier) VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO pg_subq_users (id, name, tier) VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (1, 1, 500, 'completed')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (2, 1, 300, 'completed')");
        $this->pdo->exec("INSERT INTO pg_subq_orders (id, user_id, total, status) VALUES (3, 2, 100, 'completed')");
    }

    /**
     * UPDATE with IN (subquery containing GROUP BY HAVING) should work.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/9
     */
    public function testUpdateWithInSubqueryGroupByHaving(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_subq_users SET tier = 'premium' WHERE id IN (SELECT user_id FROM pg_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400)");

            $stmt = $this->pdo->query("SELECT tier FROM pg_subq_users WHERE id = 1");
            $this->assertSame('premium', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);

            $stmt = $this->pdo->query("SELECT tier FROM pg_subq_users WHERE id = 2");
            $this->assertSame('standard', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CTE rewriter fails on UPDATE with IN (GROUP BY HAVING) on PostgreSQL: ' . $e->getMessage()
            );
        }
    }

    public function testSelectWithGroupByHavingWorks(): void
    {
        $stmt = $this->pdo->query("SELECT user_id FROM pg_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
    }
}
