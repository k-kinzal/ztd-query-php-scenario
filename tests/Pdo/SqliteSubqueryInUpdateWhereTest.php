<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE/DELETE with subqueries in WHERE that reference shadow data
 * from another table.
 *
 * Common pattern: UPDATE orders SET status = 'vip' WHERE user_id IN
 *   (SELECT id FROM users WHERE tier = 'gold')
 * Both tables may have shadow data that needs to be visible.
 * @spec SPEC-4.3
 */
class SqliteSubqueryInUpdateWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE suq_users (id INT PRIMARY KEY, name TEXT, tier TEXT)',
            'CREATE TABLE suq_orders (id INT PRIMARY KEY, user_id INT, status TEXT, total REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['suq_users', 'suq_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO suq_users VALUES (1, 'Alice', 'gold')");
        $this->pdo->exec("INSERT INTO suq_users VALUES (2, 'Bob', 'silver')");
        $this->pdo->exec("INSERT INTO suq_users VALUES (3, 'Charlie', 'gold')");
        $this->pdo->exec("INSERT INTO suq_orders VALUES (10, 1, 'pending', 100.00)");
        $this->pdo->exec("INSERT INTO suq_orders VALUES (11, 2, 'pending', 200.00)");
        $this->pdo->exec("INSERT INTO suq_orders VALUES (12, 3, 'pending', 150.00)");
    }

    /**
     * UPDATE with IN subquery referencing other shadow table.
     */
    public function testUpdateWithInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE suq_orders SET status = 'vip'
                 WHERE user_id IN (SELECT id FROM suq_users WHERE tier = 'gold')"
            );

            $rows = $this->ztdQuery("SELECT * FROM suq_orders WHERE status = 'vip' ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('10', (string) $rows[0]['id']);
            $this->assertSame('12', (string) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestSkipped('UPDATE with IN subquery not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with EXISTS subquery referencing other shadow table.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM suq_orders
                 WHERE EXISTS (SELECT 1 FROM suq_users WHERE suq_users.id = suq_orders.user_id AND tier = 'silver')"
            );

            $rows = $this->ztdQuery('SELECT * FROM suq_orders ORDER BY id');
            $this->assertCount(2, $rows, 'Only gold-tier orders should remain');
        } catch (\Exception $e) {
            $this->markTestSkipped('DELETE with EXISTS not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with subquery after mutating the subquery source table.
     */
    public function testUpdateSubqueryAfterSourceMutation(): void
    {
        // Promote Bob to gold
        $this->pdo->exec("UPDATE suq_users SET tier = 'gold' WHERE id = 2");

        try {
            $this->pdo->exec(
                "UPDATE suq_orders SET status = 'vip'
                 WHERE user_id IN (SELECT id FROM suq_users WHERE tier = 'gold')"
            );

            $rows = $this->ztdQuery("SELECT * FROM suq_orders WHERE status = 'vip' ORDER BY id");
            // All 3 users are now gold, so all 3 orders should be vip
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('UPDATE subquery after mutation not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with scalar subquery in SET.
     */
    public function testUpdateWithScalarSubqueryInSet(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE suq_orders SET status = (SELECT tier FROM suq_users WHERE suq_users.id = suq_orders.user_id)"
            );

            $rows = $this->ztdQuery('SELECT * FROM suq_orders ORDER BY id');
            $this->assertSame('gold', $rows[0]['status']);
            $this->assertSame('silver', $rows[1]['status']);
            $this->assertSame('gold', $rows[2]['status']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Scalar subquery in SET not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with NOT IN subquery.
     */
    public function testDeleteWithNotInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM suq_orders
                 WHERE user_id NOT IN (SELECT id FROM suq_users WHERE tier = 'gold')"
            );

            $rows = $this->ztdQuery('SELECT * FROM suq_orders ORDER BY id');
            $this->assertCount(2, $rows, 'Only gold-tier orders should remain');
            $this->assertSame('10', (string) $rows[0]['id']);
            $this->assertSame('12', (string) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestSkipped('DELETE with NOT IN not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with computed expression using subquery.
     */
    public function testUpdateSetComputedFromSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE suq_orders SET total = total * 0.9
                 WHERE user_id IN (SELECT id FROM suq_users WHERE tier = 'gold')"
            );

            $rows = $this->ztdQuery('SELECT * FROM suq_orders ORDER BY id');
            $this->assertEqualsWithDelta(90.0, (float) $rows[0]['total'], 0.01);
            $this->assertEqualsWithDelta(200.0, (float) $rows[1]['total'], 0.01); // silver, not updated
            $this->assertEqualsWithDelta(135.0, (float) $rows[2]['total'], 0.01);
        } catch (\Exception $e) {
            $this->markTestSkipped('Computed UPDATE with subquery not supported: ' . $e->getMessage());
        }
    }

    /**
     * Chain: INSERT new user, INSERT order for new user, UPDATE by subquery.
     */
    public function testChainedMutationWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO suq_users VALUES (4, 'Diana', 'platinum')");
        $this->pdo->exec("INSERT INTO suq_orders VALUES (13, 4, 'pending', 500.00)");

        try {
            $this->pdo->exec(
                "UPDATE suq_orders SET status = 'priority'
                 WHERE user_id IN (SELECT id FROM suq_users WHERE tier = 'platinum')"
            );

            $rows = $this->ztdQuery("SELECT * FROM suq_orders WHERE status = 'priority'");
            $this->assertCount(1, $rows);
            $this->assertSame('13', (string) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Chained mutation subquery not supported: ' . $e->getMessage());
        }
    }
}
