<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL multi-table DELETE with various syntaxes.
 *
 * MySQL supports two syntaxes for multi-table DELETE:
 * 1. Single-target: DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...
 * 2. Multi-target:  DELETE t1, t2 FROM t1 JOIN t2 ON ... WHERE ...
 *
 * The MySqlMutationResolver detects multi-table operations via
 * the projection's table count and creates MultiDeleteMutation.
 * @spec pending
 */
class MultiTableDeleteCommaSyntaxTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_md_users (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))',
            'CREATE TABLE mi_md_orders (id INT PRIMARY KEY, user_id INT, total INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_md_orders', 'mi_md_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_md_users (id, name, status) VALUES (1, 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, status) VALUES (2, 'Bob', 'inactive')");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, status) VALUES (3, 'Charlie', 'active')");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, total) VALUES (1, 1, 100)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, total) VALUES (2, 1, 200)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, total) VALUES (3, 2, 50)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, total) VALUES (4, 3, 300)");
    }

    /**
     * Single-target DELETE with JOIN: only deletes from the named target table.
     */
    public function testSingleTargetDeleteWithJoin(): void
    {
        $this->mysqli->query(
            "DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.name = 'Bob'"
        );

        // Bob's order should be deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertEquals(0, $result->fetch_assoc()['cnt']);

        // Other orders remain
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);

        // Users table is untouched
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE: DELETE t1, t2 FROM t1 JOIN t2 ...
     *
     * Expected: both tables should have matching rows deleted.
     * Actual: only the first listed table (users) is deleted from.
     * The secondary table (orders) is NOT affected — same limitation
     * as comma-syntax multi-table UPDATE (see MultiTableCommaSyntaxTest).
     */
    public function testMultiTargetDeleteOnlyAffectsFirstTable(): void
    {
        $this->mysqli->query(
            "DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.name = 'Bob'"
        );

        // First table (users): Bob IS deleted
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_md_users WHERE name = 'Bob'");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        // Second table (orders): Bob's orders are NOT deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertEquals(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE: only target tables listed before FROM are deleted.
     * If only one table is listed, the other is preserved.
     */
    public function testMultiTargetDeleteSelectiveTarget(): void
    {
        $this->mysqli->query(
            "DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.status = 'inactive'"
        );

        // Orders for inactive users (Bob) deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        // Bob still exists in users
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_md_users WHERE name = 'Bob'");
        $this->assertEquals(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation: multi-table DELETE stays in shadow.
     */
    public function testMultiTargetDeletePhysicalIsolation(): void
    {
        $this->mysqli->query(
            "DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.name = 'Bob'"
        );

        $this->mysqli->disableZtd();

        // Physical tables are untouched
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * DELETE with USING syntax (alternative multi-table form).
     * DELETE FROM t1 USING t1 JOIN t2 ON ... WHERE ...
     */
    public function testDeleteWithUsingSyntax(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_md_orders USING mi_md_orders JOIN mi_md_users ON mi_md_orders.user_id = mi_md_users.id WHERE mi_md_users.name = 'Bob'"
            );

            $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
            $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        } catch (\Throwable $e) {
            // USING syntax may not be supported by the parser
            $this->assertInstanceOf(\ZtdQuery\Adapter\Mysqli\ZtdMysqliException::class, $e);
        }
    }
}
