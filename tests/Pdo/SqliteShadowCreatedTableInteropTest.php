<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests interoperability between shadow-created tables (via CREATE TABLE
 * in ZTD mode) and reflected tables (existing physical tables). This
 * covers JOINs, INSERT...SELECT, subqueries, and aggregations across
 * both table types — a common real-world pattern where users create
 * temporary analysis tables alongside existing application tables.
 * @spec pending
 */
class SqliteShadowCreatedTableInteropTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sci_users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)',
            'CREATE TABLE sci_scores (user_id INTEGER PRIMARY KEY, score INTEGER)',
            'CREATE TABLE sci_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)',
            'CREATE TABLE sci_active_users (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE sci_blacklist (user_id INTEGER PRIMARY KEY)',
            'CREATE TABLE sci_purchases (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)',
            'CREATE TABLE sci_deactivate (user_id INTEGER PRIMARY KEY)',
            'CREATE TABLE sci_to_delete (user_id INTEGER PRIMARY KEY)',
            'CREATE TABLE sci_temp (id INTEGER PRIMARY KEY, val TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sci_users', 'sci_scores', 'sci_orders', 'sci_active_users', 'sci_blacklist', 'sci_purchases', 'sci_deactivate', 'sci_to_delete', 'sci_temp'];
    }


    /**
     * JOIN between a reflected table and a shadow-created table.
     */
    public function testJoinReflectedAndShadowCreatedTable(): void
    {
        // Insert into reflected table's shadow
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'inactive')");

        // Create a shadow table
        $this->pdo->exec('CREATE TABLE sci_scores (user_id INTEGER PRIMARY KEY, score INTEGER)');
        $this->pdo->exec("INSERT INTO sci_scores VALUES (1, 95)");
        $this->pdo->exec("INSERT INTO sci_scores VALUES (2, 72)");

        // JOIN across both
        $stmt = $this->pdo->query("
            SELECT u.name, s.score
            FROM sci_users u
            JOIN sci_scores s ON s.user_id = u.id
            ORDER BY u.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(72, (int) $rows[1]['score']);
    }

    /**
     * LEFT JOIN from reflected table to shadow-created table.
     */
    public function testLeftJoinReflectedToShadowCreated(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (3, 'Charlie', 'active')");

        $this->pdo->exec('CREATE TABLE sci_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)');
        $this->pdo->exec("INSERT INTO sci_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO sci_orders VALUES (2, 1, 50.00)");
        $this->pdo->exec("INSERT INTO sci_orders VALUES (3, 2, 75.00)");

        // LEFT JOIN — Charlie has no orders
        $stmt = $this->pdo->query("
            SELECT u.name, COUNT(o.id) AS order_count
            FROM sci_users u
            LEFT JOIN sci_orders o ON o.user_id = u.id
            GROUP BY u.id, u.name
            ORDER BY u.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['order_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['order_count']);
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertSame(0, (int) $rows[2]['order_count']);
    }

    /**
     * INSERT...SELECT from reflected table into shadow-created table.
     */
    public function testInsertSelectFromReflectedToShadowCreated(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'inactive')");

        $this->pdo->exec('CREATE TABLE sci_active_users (id INTEGER PRIMARY KEY, name TEXT)');

        $affected = $this->pdo->exec("INSERT INTO sci_active_users (id, name) SELECT id, name FROM sci_users WHERE status = 'active'");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT name FROM sci_active_users");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Subquery in WHERE referencing shadow-created table.
     */
    public function testSubqueryReferencingShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (3, 'Charlie', 'active')");

        $this->pdo->exec('CREATE TABLE sci_blacklist (user_id INTEGER PRIMARY KEY)');
        $this->pdo->exec("INSERT INTO sci_blacklist VALUES (2)");

        // Users not in blacklist
        $stmt = $this->pdo->query("
            SELECT name FROM sci_users
            WHERE id NOT IN (SELECT user_id FROM sci_blacklist)
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * Aggregation across reflected and shadow-created tables via JOIN.
     */
    public function testAggregationAcrossBothTableTypes(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'active')");

        $this->pdo->exec('CREATE TABLE sci_purchases (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)');
        $this->pdo->exec("INSERT INTO sci_purchases VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO sci_purchases VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO sci_purchases VALUES (3, 2, 50.00)");

        $stmt = $this->pdo->query("
            SELECT u.name, SUM(p.amount) AS total, AVG(p.amount) AS avg_amount
            FROM sci_users u
            JOIN sci_purchases p ON p.user_id = u.id
            GROUP BY u.id, u.name
            ORDER BY total DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(300.00, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[1]['total'], 0.01);
    }

    /**
     * UPDATE reflected table with subquery referencing shadow-created table.
     */
    public function testUpdateWithSubqueryOnShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'active')");

        $this->pdo->exec('CREATE TABLE sci_deactivate (user_id INTEGER PRIMARY KEY)');
        $this->pdo->exec("INSERT INTO sci_deactivate VALUES (2)");

        $affected = $this->pdo->exec("UPDATE sci_users SET status = 'deactivated' WHERE id IN (SELECT user_id FROM sci_deactivate)");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT status FROM sci_users WHERE id = 2");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('deactivated', $row['status']);

        // Other user unchanged
        $stmt = $this->pdo->query("SELECT status FROM sci_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('active', $row['status']);
    }

    /**
     * DELETE from reflected table with EXISTS subquery on shadow-created table.
     */
    public function testDeleteWithExistsOnShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO sci_users VALUES (3, 'Charlie', 'active')");

        $this->pdo->exec('CREATE TABLE sci_to_delete (user_id INTEGER PRIMARY KEY)');
        $this->pdo->exec("INSERT INTO sci_to_delete VALUES (1)");
        $this->pdo->exec("INSERT INTO sci_to_delete VALUES (3)");

        $affected = $this->pdo->exec("DELETE FROM sci_users WHERE EXISTS (SELECT 1 FROM sci_to_delete d WHERE d.user_id = sci_users.id)");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sci_users");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * DROP shadow-created table, then query reflected table — no side effects.
     */
    public function testDropShadowCreatedTableNoSideEffects(): void
    {
        $this->pdo->exec("INSERT INTO sci_users VALUES (1, 'Alice', 'active')");

        $this->pdo->exec('CREATE TABLE sci_temp (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO sci_temp VALUES (1, 'temp')");

        // Drop the shadow-created table
        $this->pdo->exec('DROP TABLE sci_temp');

        // Reflected table is unaffected
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sci_users");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }
}
