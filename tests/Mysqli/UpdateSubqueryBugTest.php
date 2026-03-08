<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with IN (subquery containing GROUP BY HAVING) on MySQLi.
 * Confirms that MySQL handles this pattern correctly (unlike SQLite and PostgreSQL).
 * @spec SPEC-4.2
 */
class UpdateSubqueryBugTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_subq_users (id INT PRIMARY KEY, name VARCHAR(255), tier VARCHAR(50))',
            'CREATE TABLE mi_subq_orders (id INT PRIMARY KEY, user_id INT, total DECIMAL(10,2), status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_subq_orders', 'mi_subq_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_subq_users (id, name, tier) VALUES (1, 'Alice', 'standard')");
        $this->mysqli->query("INSERT INTO mi_subq_users (id, name, tier) VALUES (2, 'Bob', 'standard')");
        $this->mysqli->query("INSERT INTO mi_subq_orders (id, user_id, total, status) VALUES (1, 1, 500, 'completed')");
        $this->mysqli->query("INSERT INTO mi_subq_orders (id, user_id, total, status) VALUES (2, 1, 300, 'completed')");
        $this->mysqli->query("INSERT INTO mi_subq_orders (id, user_id, total, status) VALUES (3, 2, 100, 'completed')");
    }

    public function testUpdateWithInSubqueryGroupByHaving(): void
    {
        // MySQL handles UPDATE with IN (subquery GROUP BY HAVING) correctly
        $this->mysqli->query("UPDATE mi_subq_users SET tier = 'premium' WHERE id IN (SELECT user_id FROM mi_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400)");

        $result = $this->mysqli->query("SELECT tier FROM mi_subq_users WHERE id = 1");
        $this->assertSame('premium', $result->fetch_assoc()['tier']);

        $result = $this->mysqli->query("SELECT tier FROM mi_subq_users WHERE id = 2");
        $this->assertSame('standard', $result->fetch_assoc()['tier']);
    }

    public function testSelectWithGroupByHavingWorks(): void
    {
        $result = $this->mysqli->query("SELECT user_id FROM mi_subq_orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(total) > 400");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['user_id']);
    }
}
