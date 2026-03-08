<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with IN (subquery containing GROUP BY HAVING) on MySQL.
 * This tests whether the CTE rewriter issue found on SQLite also affects MySQL.
 * @spec SPEC-4.2
 */
class MysqlUpdateSubqueryBugTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_subq_users (id INT PRIMARY KEY, name VARCHAR(255), tier VARCHAR(50))',
            'CREATE TABLE mysql_subq_orders (id INT PRIMARY KEY, user_id INT, total DECIMAL(10,2), status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_subq_orders', 'mysql_subq_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_subq_users (id, name, tier) VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO mysql_subq_users (id, name, tier) VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO mysql_subq_orders (id, user_id, total, status) VALUES (1, 1, 500, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_subq_orders (id, user_id, total, status) VALUES (2, 1, 300, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_subq_orders (id, user_id, total, status) VALUES (3, 2, 100, 'completed')");
    }

    public function testUpdateWithInSubqueryGroupByHaving(): void
    {
        // Test whether MySQL handles UPDATE with IN (subquery GROUP BY HAVING)
        $this->pdo->exec("UPDATE mysql_subq_users SET tier = 'premium' WHERE id IN (SELECT user_id FROM mysql_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400)");

        $stmt = $this->pdo->query("SELECT tier FROM mysql_subq_users WHERE id = 1");
        $this->assertSame('premium', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);

        $stmt = $this->pdo->query("SELECT tier FROM mysql_subq_users WHERE id = 2");
        $this->assertSame('standard', $stmt->fetch(PDO::FETCH_ASSOC)['tier']);
    }
}
