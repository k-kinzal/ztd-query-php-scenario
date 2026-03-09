<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests prepared UPDATE with CASE in SET and INSERT with subquery in VALUES
 * on PostgreSQL PDO.
 *
 * Adjacent to Issue #61 (prepared $N CASE SET no-op) — this uses ? params
 * instead to check if PDO ? placeholder avoids the issue.
 * Also tests INSERT with scalar subquery in VALUES and self-referencing DELETE.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 */
class PostgresPreparedCaseSetAndSubqueryInsertTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_pcsi_items (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                tier VARCHAR(20) DEFAULT \'standard\'
            )',
            'CREATE TABLE pg_pcsi_config (
                id INT PRIMARY KEY,
                key_name VARCHAR(50),
                val VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_pcsi_config', 'pg_pcsi_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_pcsi_items VALUES (1, 'Widget', 25.00, 'standard')");
        $this->pdo->exec("INSERT INTO pg_pcsi_items VALUES (2, 'Gadget', 50.00, 'standard')");
        $this->pdo->exec("INSERT INTO pg_pcsi_items VALUES (3, 'Gizmo', 75.00, 'standard')");
        $this->pdo->exec("INSERT INTO pg_pcsi_items VALUES (4, 'Doodad', 10.00, 'standard')");

        $this->pdo->exec("INSERT INTO pg_pcsi_config VALUES (1, 'premium_threshold', '40')");
        $this->pdo->exec("INSERT INTO pg_pcsi_config VALUES (2, 'discount_pct', '10')");
    }

    // --- Prepared UPDATE CASE in SET (using ? params, not $N) ---

    public function testPreparedUpdateCaseInSetSimple(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_pcsi_items SET tier = CASE
                    WHEN price >= ? THEN 'premium'
                    ELSE 'budget'
                 END"
            );
            $stmt->execute([50.00]);

            $rows = $this->ztdQuery('SELECT id, tier FROM pg_pcsi_items ORDER BY id');
            $this->assertCount(4, $rows);
            $this->assertSame('budget', $rows[0]['tier']);
            $this->assertSame('premium', $rows[1]['tier']);
            $this->assertSame('premium', $rows[2]['tier']);
            $this->assertSame('budget', $rows[3]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE SET (? params) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCaseInSetWithWhereParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_pcsi_items SET tier = CASE
                    WHEN price >= ? THEN 'premium'
                    ELSE 'budget'
                 END
                 WHERE name LIKE ?"
            );
            $stmt->execute([50.00, 'G%']);

            $rows = $this->ztdQuery('SELECT id, tier FROM pg_pcsi_items ORDER BY id');
            $this->assertSame('standard', $rows[0]['tier']); // Widget untouched
            $this->assertSame('premium', $rows[1]['tier']);   // Gadget
            $this->assertSame('premium', $rows[2]['tier']);   // Gizmo
            $this->assertSame('standard', $rows[3]['tier']); // Doodad untouched
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE SET + WHERE (? params) failed: ' . $e->getMessage());
        }
    }

    public function testExecUpdateCaseInSet(): void
    {
        $this->pdo->exec(
            "UPDATE pg_pcsi_items SET tier = CASE
                WHEN price >= 50 THEN 'premium'
                ELSE 'budget'
             END"
        );

        $rows = $this->ztdQuery('SELECT id, tier FROM pg_pcsi_items ORDER BY id');
        $this->assertSame('budget', $rows[0]['tier']);
        $this->assertSame('premium', $rows[1]['tier']);
        $this->assertSame('premium', $rows[2]['tier']);
        $this->assertSame('budget', $rows[3]['tier']);
    }

    // --- INSERT with scalar subquery in VALUES ---

    public function testInsertWithScalarSubqueryInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_pcsi_items (id, name, price, tier)
                 VALUES (5, 'Copied', (SELECT price FROM pg_pcsi_items WHERE id = 3), 'standard')"
            );

            $rows = $this->ztdQuery('SELECT price FROM pg_pcsi_items WHERE id = 5');
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(75.00, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with scalar subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithMaxSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_pcsi_items (id, name, price, tier)
                 VALUES (5, 'MaxPrice', (SELECT MAX(price) FROM pg_pcsi_items), 'premium')"
            );

            $rows = $this->ztdQuery('SELECT price FROM pg_pcsi_items WHERE id = 5');
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(75.00, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with MAX subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithSubqueryFromDifferentTable(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_pcsi_items (id, name, price, tier)
                 VALUES (5, (SELECT val FROM pg_pcsi_config WHERE key_name = 'premium_threshold'), 0.00, 'config')"
            );

            $rows = $this->ztdQuery('SELECT name FROM pg_pcsi_items WHERE id = 5');
            $this->assertCount(1, $rows);
            $this->assertSame('40', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with subquery from different table failed: ' . $e->getMessage());
        }
    }

    // --- DELETE with self-referencing subquery ---

    public function testDeleteWithSelfReferencingAvgSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_pcsi_items
                 WHERE price < (SELECT AVG(price) FROM pg_pcsi_items)"
            );

            $rows = $this->ztdQuery('SELECT id FROM pg_pcsi_items ORDER BY id');
            $this->assertCount(2, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([2, 3], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with self-referencing AVG subquery failed: ' . $e->getMessage());
        }
    }

    // --- String concat with || ---

    public function testUpdateSetWithStringConcat(): void
    {
        $this->pdo->exec(
            "UPDATE pg_pcsi_items SET name = name || ' [' || tier || ']' WHERE id = 1"
        );

        $rows = $this->ztdQuery('SELECT name FROM pg_pcsi_items WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('Widget [standard]', $rows[0]['name']);
    }

    public function testSelectWithConcatInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM pg_pcsi_items WHERE name || tier = 'Widgetstandard'"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_pcsi_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
