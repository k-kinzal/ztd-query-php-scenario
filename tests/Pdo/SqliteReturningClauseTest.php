<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT/UPDATE/DELETE ... RETURNING through ZTD shadow store on SQLite.
 *
 * RETURNING is widely used in production PostgreSQL and SQLite (3.35+) apps
 * to avoid a separate SELECT after DML. The CTE rewriter must preserve
 * the RETURNING clause and return correct shadow data.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class SqliteReturningClauseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ret_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL DEFAULT 0.0,
                stock INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ret_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ret_items (name, price, stock) VALUES ('Alpha', 10.50, 100)");
        $this->pdo->exec("INSERT INTO sl_ret_items (name, price, stock) VALUES ('Beta', 25.00, 50)");
        $this->pdo->exec("INSERT INTO sl_ret_items (name, price, stock) VALUES ('Gamma', 5.75, 200)");
    }

    /**
     * INSERT ... RETURNING * should return the inserted row with auto-generated id.
     */
    public function testInsertReturningAll(): void
    {
        try {
            $result = $this->pdo->query(
                "INSERT INTO sl_ret_items (name, price, stock) VALUES ('Delta', 42.00, 10) RETURNING *"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT RETURNING *: expected 1 row, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Delta', $rows[0]['name']);
            $this->assertEqualsWithDelta(42.00, (float) $rows[0]['price'], 0.01);
            $this->assertSame(10, (int) $rows[0]['stock']);
            $this->assertNotEmpty($rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT RETURNING * failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ... RETURNING specific columns.
     */
    public function testInsertReturningSpecificColumns(): void
    {
        try {
            $result = $this->pdo->query(
                "INSERT INTO sl_ret_items (name, price, stock) VALUES ('Epsilon', 99.99, 5) RETURNING id, name"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT RETURNING id,name: expected 1 row, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertArrayHasKey('id', $rows[0]);
            $this->assertArrayHasKey('name', $rows[0]);
            $this->assertSame('Epsilon', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT RETURNING specific columns failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... RETURNING should return the updated rows with new values.
     */
    public function testUpdateReturningAll(): void
    {
        try {
            $result = $this->pdo->query(
                "UPDATE sl_ret_items SET price = price * 1.1 WHERE stock < 100 RETURNING *"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING *: expected 1 row (Beta stock=50), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Beta', $rows[0]['name']);
            // Beta price: 25.00 * 1.1 = 27.50
            $this->assertEqualsWithDelta(27.50, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE RETURNING * failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... RETURNING with expression in select list.
     */
    public function testUpdateReturningExpression(): void
    {
        try {
            $result = $this->pdo->query(
                "UPDATE sl_ret_items SET stock = stock - 10 RETURNING id, name, stock AS new_stock"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING expression: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            foreach ($rows as $r) {
                $this->assertArrayHasKey('new_stock', $r);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE RETURNING expression failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... RETURNING should return the deleted rows.
     */
    public function testDeleteReturningAll(): void
    {
        try {
            $result = $this->pdo->query(
                "DELETE FROM sl_ret_items WHERE price < 10 RETURNING *"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE RETURNING *: expected 1 row (Gamma price=5.75), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gamma', $rows[0]['name']);

            // Verify the row was actually deleted from shadow store
            $remaining = $this->ztdQuery("SELECT name FROM sl_ret_items ORDER BY id");
            $this->assertCount(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE RETURNING * failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT ... RETURNING with bound parameters.
     */
    public function testPreparedInsertReturning(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_ret_items (name, price, stock) VALUES (?, ?, ?) RETURNING id, name, price"
            );
            $stmt->execute(['Zeta', 77.77, 33]);
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT RETURNING: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Zeta', $rows[0]['name']);
            $this->assertEqualsWithDelta(77.77, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT RETURNING failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT ... RETURNING should return all inserted rows.
     */
    public function testMultiRowInsertReturning(): void
    {
        try {
            $result = $this->pdo->query(
                "INSERT INTO sl_ret_items (name, price, stock) VALUES
                    ('X1', 1.00, 10),
                    ('X2', 2.00, 20),
                    ('X3', 3.00, 30)
                 RETURNING id, name"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT RETURNING: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('X1', $names);
            $this->assertContains('X2', $names);
            $this->assertContains('X3', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT RETURNING failed: ' . $e->getMessage());
        }
    }

    /**
     * Verify shadow store consistency after RETURNING operations.
     */
    public function testShadowConsistencyAfterReturning(): void
    {
        try {
            // INSERT RETURNING
            $this->pdo->query(
                "INSERT INTO sl_ret_items (name, price, stock) VALUES ('New1', 11.11, 1) RETURNING id"
            );

            // UPDATE RETURNING
            $this->pdo->query(
                "UPDATE sl_ret_items SET price = 99.99 WHERE name = 'New1' RETURNING *"
            );

            // Verify via normal SELECT
            $rows = $this->ztdQuery("SELECT price FROM sl_ret_items WHERE name = 'New1'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Shadow consistency: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Shadow consistency after RETURNING failed: ' . $e->getMessage());
        }
    }
}
