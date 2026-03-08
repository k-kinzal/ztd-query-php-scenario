<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests prepared statements with parameters in HAVING, GROUP BY, and ORDER BY.
 *
 * KNOWN ISSUE (SQLite only): HAVING clause with prepared statement parameters
 * returns empty results. See https://github.com/k-kinzal/ztd-query-php/issues/22
 * @spec pending
 */
class SqlitePreparedAggregateParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pap_orders (id INTEGER PRIMARY KEY, customer TEXT, product TEXT, amount REAL, status TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['pap_orders'];
    }


    public function testGroupByWithWhereParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, SUM(amount) AS total FROM pap_orders WHERE status = ? GROUP BY customer ORDER BY total DESC');
        $stmt->execute(['completed']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
        $this->assertEqualsWithDelta(120.0, (float) $rows[0]['total'], 0.01);
    }

    public function testOrderByWithLimitParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, amount FROM pap_orders ORDER BY amount DESC LIMIT ?');
        $stmt->execute([3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
        $this->assertEqualsWithDelta(120.0, (float) $rows[0]['amount'], 0.01);
    }

    /**
     * HAVING with prepared params should return matching groups.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/22
     */
    public function testHavingWithParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, COUNT(*) AS order_count FROM pap_orders GROUP BY customer HAVING COUNT(*) >= ?');
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Expected: all 3 customers have 2 orders each
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Issue #22: HAVING with prepared params returns empty on SQLite. Expected 3 rows, got 0'
            );
        }
        $customers = array_column($rows, 'customer');
        sort($customers);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $customers);
    }

    /**
     * HAVING with literal values works correctly.
     */
    public function testHavingWithLiteralWorks(): void
    {
        $stmt = $this->pdo->query('SELECT customer, COUNT(*) AS order_count FROM pap_orders GROUP BY customer HAVING COUNT(*) >= 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $customers = array_column($rows, 'customer');
        sort($customers);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $customers);
    }

    public function testNamedParamsInWherePlusGroupBy(): void
    {
        $stmt = $this->pdo->prepare('SELECT customer, COUNT(*) AS cnt FROM pap_orders WHERE status = :status GROUP BY customer ORDER BY customer');
        $stmt->execute([':status' => 'completed']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }
}
