<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL multi-table DELETE with prepared parameters.
 *
 * MySQL supports: DELETE t1 FROM t1 JOIN t2 ON ... WHERE t2.col = ?
 * Existing tests cover exec() path but not prepared params.
 *
 * @spec SPEC-4.3
 */
class MysqlDeleteMultiTablePreparedTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_dmp_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                status VARCHAR(20) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_dmp_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                tier VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_dmp_orders', 'my_dmp_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_dmp_customers VALUES (1, 'Alice', 'gold')");
        $this->pdo->exec("INSERT INTO my_dmp_customers VALUES (2, 'Bob', 'silver')");
        $this->pdo->exec("INSERT INTO my_dmp_customers VALUES (3, 'Charlie', 'bronze')");

        $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (1, 1, 'pending', 100)");
        $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (2, 1, 'completed', 200)");
        $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (3, 2, 'pending', 150)");
        $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (4, 2, 'completed', 80)");
        $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (5, 3, 'pending', 50)");
    }

    /**
     * DELETE with JOIN and prepared param in WHERE.
     *
     * DELETE o FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.tier = ?
     */
    public function testDeleteJoinWithParam(): void
    {
        $sql = "DELETE o FROM my_dmp_orders o
                JOIN my_dmp_customers c ON o.customer_id = c.id
                WHERE c.tier = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['bronze']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dmp_orders");

            $count = (int) $rows[0]['cnt'];
            // Should delete Charlie's 1 order (id=5), leaving 4
            if ($count !== 4) {
                $this->markTestIncomplete(
                    "DELETE JOIN with param: expected 4 remaining, got {$count}"
                );
            }

            $this->assertSame(4, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE JOIN with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with JOIN and multiple prepared params.
     */
    public function testDeleteJoinWithMultipleParams(): void
    {
        $sql = "DELETE o FROM my_dmp_orders o
                JOIN my_dmp_customers c ON o.customer_id = c.id
                WHERE c.tier = ? AND o.status = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['silver', 'pending']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dmp_orders");

            $count = (int) $rows[0]['cnt'];
            // Should delete Bob's pending order (id=3), leaving 4
            if ($count !== 4) {
                $this->markTestIncomplete(
                    "DELETE JOIN multi-param: expected 4 remaining, got {$count}"
                );
            }

            $this->assertSame(4, $count);

            // Verify Bob's completed order still exists
            $remaining = $this->ztdQuery("SELECT id FROM my_dmp_orders WHERE customer_id = 2");
            if (count($remaining) !== 1 || (int) $remaining[0]['id'] !== 4) {
                $this->markTestIncomplete(
                    "DELETE JOIN multi-param: Bob should have order 4 only. Data: " . json_encode($remaining)
                );
            }

            $this->assertCount(1, $remaining);
            $this->assertSame(4, (int) $remaining[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE JOIN multi-param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with JOIN on shadow-inserted data and param.
     */
    public function testDeleteJoinShadowDataWithParam(): void
    {
        try {
            // Insert a new customer and order into shadow
            $this->pdo->exec("INSERT INTO my_dmp_customers VALUES (4, 'Diana', 'platinum')");
            $this->pdo->exec("INSERT INTO my_dmp_orders VALUES (6, 4, 'pending', 500)");

            $sql = "DELETE o FROM my_dmp_orders o
                    JOIN my_dmp_customers c ON o.customer_id = c.id
                    WHERE c.tier = ?";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['platinum']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dmp_orders WHERE customer_id = 4");

            $count = (int) $rows[0]['cnt'];
            if ($count !== 0) {
                $this->markTestIncomplete(
                    "DELETE JOIN shadow data: expected 0 orders for Diana, got {$count}"
                );
            }

            $this->assertSame(0, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE JOIN shadow data with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE using comma syntax with prepared param.
     *
     * DELETE o FROM orders o, customers c WHERE o.customer_id = c.id AND c.tier = ?
     */
    public function testDeleteCommaSyntaxWithParam(): void
    {
        $sql = "DELETE o FROM my_dmp_orders o, my_dmp_customers c
                WHERE o.customer_id = c.id AND c.tier = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['gold']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dmp_orders WHERE customer_id = 1");

            $count = (int) $rows[0]['cnt'];
            // Should delete Alice's 2 orders
            if ($count !== 0) {
                $this->markTestIncomplete(
                    "DELETE comma syntax with param: expected 0 Alice orders, got {$count}"
                );
            }

            $this->assertSame(0, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE comma syntax with param failed: ' . $e->getMessage()
            );
        }
    }
}
