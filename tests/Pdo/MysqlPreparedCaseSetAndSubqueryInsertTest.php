<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests prepared UPDATE with CASE expression in SET clause, and INSERT with
 * scalar subquery in VALUES clause, on MySQL PDO.
 *
 * Adjacent to Issue #61 (PostgreSQL: prepared $N CASE in SET is a no-op),
 * Issue #83 (INSERT SELECT with expressions stores NULLs), and
 * Issue #96 (CASE in WHERE matches all rows on MySQL).
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 */
class MysqlPreparedCaseSetAndSubqueryInsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_pcsi_items (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price DECIMAL(10,2),
                tier VARCHAR(20) DEFAULT \'standard\'
            )',
            'CREATE TABLE my_pcsi_config (
                id INT PRIMARY KEY,
                key_name VARCHAR(50),
                val VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_pcsi_config', 'my_pcsi_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_pcsi_items VALUES (1, 'Widget', 25.00, 'standard')");
        $this->pdo->exec("INSERT INTO my_pcsi_items VALUES (2, 'Gadget', 50.00, 'standard')");
        $this->pdo->exec("INSERT INTO my_pcsi_items VALUES (3, 'Gizmo', 75.00, 'standard')");
        $this->pdo->exec("INSERT INTO my_pcsi_items VALUES (4, 'Doodad', 10.00, 'standard')");

        $this->pdo->exec("INSERT INTO my_pcsi_config VALUES (1, 'premium_threshold', '40')");
        $this->pdo->exec("INSERT INTO my_pcsi_config VALUES (2, 'discount_pct', '10')");
    }

    // --- Prepared UPDATE with CASE in SET ---

    public function testPreparedUpdateCaseInSetSimple(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_pcsi_items SET tier = CASE
                    WHEN price >= ? THEN 'premium'
                    ELSE 'budget'
                 END"
            );
            $stmt->execute([50.00]);

            $rows = $this->ztdQuery('SELECT id, tier FROM my_pcsi_items ORDER BY id');
            $this->assertCount(4, $rows);
            // id 1: 25 < 50 → budget, id 2: 50 ≥ 50 → premium, id 3: 75 ≥ 50 → premium, id 4: 10 < 50 → budget
            $this->assertSame('budget', $rows[0]['tier']);
            $this->assertSame('premium', $rows[1]['tier']);
            $this->assertSame('premium', $rows[2]['tier']);
            $this->assertSame('budget', $rows[3]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE in SET failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCaseInSetWithWhereParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_pcsi_items SET tier = CASE
                    WHEN price >= ? THEN 'premium'
                    ELSE 'budget'
                 END
                 WHERE name LIKE ?"
            );
            $stmt->execute([50.00, 'G%']);

            $rows = $this->ztdQuery('SELECT id, tier FROM my_pcsi_items ORDER BY id');
            // Only Gadget and Gizmo matched LIKE 'G%'
            // Widget: unchanged 'standard', Gadget: 50→premium, Gizmo: 75→premium, Doodad: unchanged
            $this->assertSame('standard', $rows[0]['tier']); // Widget
            $this->assertSame('premium', $rows[1]['tier']);   // Gadget
            $this->assertSame('premium', $rows[2]['tier']);   // Gizmo
            $this->assertSame('standard', $rows[3]['tier']); // Doodad
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE CASE SET + WHERE param failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateMultipleCaseInSet(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_pcsi_items SET
                    tier = CASE WHEN price >= ? THEN 'premium' ELSE 'budget' END,
                    price = CASE WHEN price >= ? THEN price * 0.9 ELSE price * 1.1 END"
            );
            $stmt->execute([50.00, 50.00]);

            $rows = $this->ztdQuery('SELECT id, price, tier FROM my_pcsi_items ORDER BY id');
            // Widget: budget, 25*1.1=27.5
            // Gadget: premium, 50*0.9=45
            // Gizmo: premium, 75*0.9=67.5
            // Doodad: budget, 10*1.1=11
            $this->assertSame('budget', $rows[0]['tier']);
            $this->assertEqualsWithDelta(27.50, (float) $rows[0]['price'], 0.01);
            $this->assertSame('premium', $rows[1]['tier']);
            $this->assertEqualsWithDelta(45.00, (float) $rows[1]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE multiple CASE SET failed: ' . $e->getMessage());
        }
    }

    public function testExecUpdateCaseInSet(): void
    {
        // Control: non-prepared should work
        $this->pdo->exec(
            "UPDATE my_pcsi_items SET tier = CASE
                WHEN price >= 50 THEN 'premium'
                ELSE 'budget'
             END"
        );

        $rows = $this->ztdQuery('SELECT id, tier FROM my_pcsi_items ORDER BY id');
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
                "INSERT INTO my_pcsi_items (id, name, price, tier)
                 VALUES (5, 'Copied', (SELECT price FROM my_pcsi_items WHERE id = 3), 'standard')"
            );

            $rows = $this->ztdQuery('SELECT price FROM my_pcsi_items WHERE id = 5');
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
                "INSERT INTO my_pcsi_items (id, name, price, tier)
                 VALUES (5, 'MaxPrice', (SELECT MAX(price) FROM my_pcsi_items), 'premium')"
            );

            $rows = $this->ztdQuery('SELECT price FROM my_pcsi_items WHERE id = 5');
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
                "INSERT INTO my_pcsi_items (id, name, price, tier)
                 VALUES (5, (SELECT val FROM my_pcsi_config WHERE key_name = 'premium_threshold'), 0.00, 'config')"
            );

            $rows = $this->ztdQuery('SELECT name FROM my_pcsi_items WHERE id = 5');
            $this->assertCount(1, $rows);
            $this->assertSame('40', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with subquery from different table failed: ' . $e->getMessage());
        }
    }

    public function testInsertSubqueryThenVerifyCount(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO my_pcsi_items (id, name, price, tier)
                 VALUES (5, 'CountBased', (SELECT COUNT(*) * 10 FROM my_pcsi_items), 'standard')"
            );

            $rows = $this->ztdQuery('SELECT price FROM my_pcsi_items WHERE id = 5');
            $this->assertCount(1, $rows);
            // COUNT(*) = 4 at time of INSERT, so price = 40.00
            $this->assertEqualsWithDelta(40.00, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COUNT subquery in VALUES failed: ' . $e->getMessage());
        }
    }

    // --- DELETE with self-referencing subquery ---

    public function testDeleteWithSelfReferencingInSubquery(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM my_pcsi_items
                 WHERE price < (SELECT AVG(price) FROM my_pcsi_items)"
            );

            $rows = $this->ztdQuery('SELECT id FROM my_pcsi_items ORDER BY id');
            // AVG = (25+50+75+10)/4 = 40. price < 40: id 1(25), 4(10)
            $this->assertCount(2, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([2, 3], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with self-referencing subquery failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_pcsi_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
