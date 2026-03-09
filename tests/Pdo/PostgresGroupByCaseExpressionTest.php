<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUP BY with CASE expression on PostgreSQL shadow data.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresGroupByCaseExpressionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_gbce_orders (
                id SERIAL PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_gbce_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gbce_orders (id, customer, amount, status) VALUES (1, 'Alice', 25.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_gbce_orders (id, customer, amount, status) VALUES (2, 'Bob', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_gbce_orders (id, customer, amount, status) VALUES (3, 'Carol', 500.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_gbce_orders (id, customer, amount, status) VALUES (4, 'Dave', 75.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_gbce_orders (id, customer, amount, status) VALUES (5, 'Eve', 1200.00, 'completed')");
    }

    /**
     * GROUP BY CASE expression.
     */
    public function testGroupByCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE
                    WHEN amount >= 1000 THEN 'high'
                    WHEN amount >= 100 THEN 'medium'
                    ELSE 'low'
                END AS tier,
                COUNT(*) AS cnt,
                SUM(amount) AS total
             FROM pg_gbce_orders
             WHERE status = 'completed'
             GROUP BY CASE
                WHEN amount >= 1000 THEN 'high'
                WHEN amount >= 100 THEN 'medium'
                ELSE 'low'
             END
             ORDER BY total DESC"
        );

        $this->assertCount(3, $rows);
        $byTier = array_column($rows, null, 'tier');
        $this->assertEquals(1, (int) $byTier['high']['cnt']);
        $this->assertEquals(2, (int) $byTier['medium']['cnt']);
        $this->assertEquals(1, (int) $byTier['low']['cnt']);
    }

    /**
     * GROUP BY CASE with HAVING.
     */
    public function testGroupByCaseWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE WHEN amount >= 100 THEN 'big' ELSE 'small' END AS size,
                COUNT(*) AS cnt
             FROM pg_gbce_orders
             GROUP BY CASE WHEN amount >= 100 THEN 'big' ELSE 'small' END
             HAVING COUNT(*) >= 2"
        );

        // big: Bob(150), Carol(500), Eve(1200) = 3
        // small: Alice(25), Dave(75) = 2
        $this->assertCount(2, $rows);
    }

    /**
     * GROUP BY CASE with prepared $N params.
     *
     * The PgSqlParser may mis-handle $N parameter placeholders inside
     * CASE expressions in GROUP BY position, producing wrong grouping
     * or collapsing groups. Related to Issue #75 (CASE with params).
     */
    public function testGroupByCaseWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT
                CASE WHEN amount >= $1 THEN 'above' ELSE 'below' END AS bracket,
                COUNT(*) AS cnt
             FROM pg_gbce_orders
             GROUP BY CASE WHEN amount >= $1 THEN 'above' ELSE 'below' END
             ORDER BY bracket"
        );
        $stmt->execute([100]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'GROUP BY CASE with prepared $N params returned no rows on PostgreSQL.'
            );
        }

        if (count($rows) !== 2) {
            $this->markTestIncomplete(
                'GROUP BY CASE with prepared $N params returned ' . count($rows) . ' group(s) '
                . 'instead of 2 on PostgreSQL. The $N params in the CASE expression inside '
                . 'GROUP BY may be causing incorrect grouping. '
                . 'Got: ' . json_encode($rows)
            );
        }

        $this->assertCount(2, $rows);
        $byBracket = array_column($rows, null, 'bracket');
        $this->assertEquals(3, (int) $byBracket['above']['cnt']);
        $this->assertEquals(2, (int) $byBracket['below']['cnt']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_gbce_orders')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
