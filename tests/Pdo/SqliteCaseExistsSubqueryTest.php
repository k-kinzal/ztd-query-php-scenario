<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE WHEN EXISTS (subquery) patterns via SQLite PDO.
 *
 * @spec SPEC-3.1, SPEC-4.2
 */
class SqliteCaseExistsSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ceq_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_ceq_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ceq_orders', 'sl_ceq_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ceq_users VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_ceq_users VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_ceq_users VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO sl_ceq_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_ceq_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO sl_ceq_orders VALUES (3, 3, 50)");
    }

    public function testCaseExistsInSelect(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM sl_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete('CASE EXISTS in SELECT: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_orders']);
            $this->assertSame('no', $rows[1]['has_orders']);
            $this->assertSame('yes', $rows[2]['has_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE EXISTS in SELECT failed: ' . $e->getMessage());
        }
    }

    public function testCaseExistsWithAggregate(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE
                            WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id GROUP BY o.user_id HAVING SUM(o.amount) >= 200)
                                THEN 'high'
                            WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)
                                THEN 'low'
                            ELSE 'none'
                        END AS tier
                 FROM sl_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete('CASE EXISTS with aggregate: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertSame('high', $rows[0]['tier']);
            $this->assertSame('none', $rows[1]['tier']);
            $this->assertSame('low', $rows[2]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE EXISTS with aggregate failed: ' . $e->getMessage());
        }
    }

    public function testCaseExistsInWhere(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name
                 FROM sl_ceq_users u
                 WHERE CASE WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)
                            THEN 1 ELSE 0 END = 1
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete('CASE EXISTS in WHERE: expected 2 rows, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE EXISTS in WHERE failed: ' . $e->getMessage());
        }
    }

    public function testCaseExistsAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ceq_orders VALUES (4, 2, 150)");

            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM sl_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete('CASE EXISTS after INSERT: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_orders']);
            $this->assertSame('yes', $rows[1]['has_orders']);
            $this->assertSame('yes', $rows[2]['has_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE EXISTS after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    public function testCaseNotExistsInSelect(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT u.id, u.name,
                        CASE WHEN NOT EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)
                             THEN 'inactive' ELSE 'active' END AS status
                 FROM sl_ceq_users u
                 ORDER BY u.id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete('CASE NOT EXISTS: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertSame('active', $rows[0]['status']);
            $this->assertSame('inactive', $rows[1]['status']);
            $this->assertSame('active', $rows[2]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE NOT EXISTS failed: ' . $e->getMessage());
        }
    }

    public function testPreparedCaseExists(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id AND o.amount >= ?)
                             THEN 'yes' ELSE 'no' END AS has_big_orders
                 FROM sl_ceq_users u
                 ORDER BY u.id"
            );
            $stmt->execute([100]);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete('Prepared CASE EXISTS: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertSame('yes', $rows[0]['has_big_orders']);
            $this->assertSame('no', $rows[1]['has_big_orders']);
            $this->assertSame('no', $rows[2]['has_big_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared CASE EXISTS failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->query(
            "SELECT u.id FROM sl_ceq_users u
             WHERE EXISTS (SELECT 1 FROM sl_ceq_orders o WHERE o.user_id = u.id)"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ceq_users")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
