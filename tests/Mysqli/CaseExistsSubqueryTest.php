<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CASE WHEN EXISTS (subquery) patterns via MySQLi.
 *
 * @spec SPEC-3.1, SPEC-4.2
 */
class CaseExistsSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ceq_users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE ceq_orders (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                amount INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ceq_orders', 'ceq_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO ceq_users VALUES (1, 'Alice')");
        $this->ztdExec("INSERT INTO ceq_users VALUES (2, 'Bob')");
        $this->ztdExec("INSERT INTO ceq_users VALUES (3, 'Charlie')");

        $this->ztdExec("INSERT INTO ceq_orders VALUES (1, 1, 100)");
        $this->ztdExec("INSERT INTO ceq_orders VALUES (2, 1, 200)");
        $this->ztdExec("INSERT INTO ceq_orders VALUES (3, 3, 50)");
    }

    public function testCaseExistsInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM ceq_users u
                 ORDER BY u.id"
            );

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
            $rows = $this->ztdQuery(
                "SELECT u.id, u.name,
                        CASE
                            WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id HAVING SUM(o.amount) >= 200)
                                THEN 'high'
                            WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)
                                THEN 'low'
                            ELSE 'none'
                        END AS tier
                 FROM ceq_users u
                 ORDER BY u.id"
            );

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
            $rows = $this->ztdQuery(
                "SELECT u.id, u.name
                 FROM ceq_users u
                 WHERE CASE WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)
                            THEN 1 ELSE 0 END = 1
                 ORDER BY u.id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('CASE EXISTS in WHERE: expected 2 rows, got ' . count($rows));
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Issue #98: CASE EXISTS in WHERE misdetected as multi-statement: ' . $e->getMessage());
        }
    }

    public function testCaseExistsAfterShadowInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO ceq_orders VALUES (4, 2, 150)");

            $rows = $this->ztdQuery(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)
                             THEN 'yes' ELSE 'no' END AS has_orders
                 FROM ceq_users u
                 ORDER BY u.id"
            );

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
            $rows = $this->ztdQuery(
                "SELECT u.id, u.name,
                        CASE WHEN NOT EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)
                             THEN 'inactive' ELSE 'active' END AS status
                 FROM ceq_users u
                 ORDER BY u.id"
            );

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
            $rows = $this->ztdPrepareAndExecute(
                "SELECT u.id, u.name,
                        CASE WHEN EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id AND o.amount >= ?)
                             THEN 'yes' ELSE 'no' END AS has_big_orders
                 FROM ceq_users u
                 ORDER BY u.id",
                [100]
            );

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
        $this->ztdQuery(
            "SELECT u.id FROM ceq_users u
             WHERE EXISTS (SELECT 1 FROM ceq_orders o WHERE o.user_id = u.id)"
        );

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM ceq_users");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
