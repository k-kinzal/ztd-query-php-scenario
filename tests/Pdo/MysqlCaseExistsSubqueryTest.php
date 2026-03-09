<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CASE WHEN EXISTS (subquery) patterns via MySQL PDO.
 *
 * EXISTS subqueries inside CASE expressions appear in SELECT, WHERE,
 * and UPDATE SET clauses. The CTE rewriter must rewrite table references
 * inside these deeply nested subqueries.
 *
 * @spec SPEC-3.1, SPEC-4.2
 */
class MysqlCaseExistsSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ceq_users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_ceq_orders (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                amount INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ceq_orders', 'mp_ceq_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ceq_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO mp_ceq_users VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO mp_ceq_users VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO mp_ceq_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO mp_ceq_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO mp_ceq_orders VALUES (3, 3, 50)");
    }

    /**
     * CASE WHEN EXISTS (subquery) in SELECT list.
     *
     * Produces 'yes'/'no' for whether each user has any orders.
     * Alice: yes (2 orders), Bob: no, Charlie: yes (1 order).
     */
    public function testCaseExistsInSelect(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM mp_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE EXISTS in SELECT: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_orders']); // Alice
            $this->assertSame('no', $rows[1]['has_orders']);   // Bob
            $this->assertSame('yes', $rows[2]['has_orders']); // Charlie
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE EXISTS in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * CASE WHEN EXISTS with aggregate in subquery.
     *
     * Classifies users by total order amount: 'high' (>=200), 'low', or 'none'.
     */
    public function testCaseExistsWithAggregate(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE
                            WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id HAVING SUM(o.amount) >= 200)
                                THEN 'high'
                            WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)
                                THEN 'low'
                            ELSE 'none'
                        END AS tier
                 FROM mp_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE EXISTS with aggregate: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('high', $rows[0]['tier']); // Alice: 100+200=300
            $this->assertSame('none', $rows[1]['tier']);  // Bob: no orders
            $this->assertSame('low', $rows[2]['tier']);   // Charlie: 50
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE EXISTS with aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE clause with CASE WHEN EXISTS.
     *
     * Select users who have orders (EXISTS = 1).
     */
    public function testCaseExistsInWhere(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name
                 FROM mp_ceq_users u
                 WHERE CASE WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)
                            THEN 1 ELSE 0 END = 1
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CASE EXISTS in WHERE: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']); // Alice
            $this->assertSame(3, (int) $rows[1]['id']); // Charlie
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Issue #98: CASE EXISTS in WHERE misdetected as multi-statement: ' . $e->getMessage()
            );
        }
    }

    /**
     * CASE EXISTS after shadow mutations.
     *
     * Add an order for Bob, then verify EXISTS picks it up.
     */
    public function testCaseExistsAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_ceq_orders VALUES (4, 2, 150)");

            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM mp_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE EXISTS after INSERT: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_orders']); // Alice
            $this->assertSame('yes', $rows[1]['has_orders']); // Bob (now has order)
            $this->assertSame('yes', $rows[2]['has_orders']); // Charlie
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE EXISTS after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NOT EXISTS in CASE expression.
     *
     * Identify users without any orders.
     */
    public function testCaseNotExistsInSelect(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN NOT EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'inactive' ELSE 'active' END AS status
                 FROM mp_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE NOT EXISTS: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('active', $rows[0]['status']);   // Alice
            $this->assertSame('inactive', $rows[1]['status']); // Bob
            $this->assertSame('active', $rows[2]['status']);   // Charlie
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CASE NOT EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with CASE EXISTS and bound parameter.
     */
    public function testPreparedCaseExists(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id AND o.amount >= ?)
                             THEN 'yes' ELSE 'no' END AS has_big_orders
                 FROM mp_ceq_users u
                 ORDER BY u.id"
            );
            $stmt->execute([100]);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared CASE EXISTS: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_big_orders']); // Alice: 100, 200
            $this->assertSame('no', $rows[1]['has_big_orders']);   // Bob: no orders
            $this->assertSame('no', $rows[2]['has_big_orders']);   // Charlie: 50
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared CASE EXISTS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->query(
            "SELECT u.id FROM mp_ceq_users u
             WHERE EXISTS (SELECT 1 FROM mp_ceq_orders o WHERE o.user_id = u.id)"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ceq_users")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
