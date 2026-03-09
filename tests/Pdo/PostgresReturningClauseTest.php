<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use Tests\Support\PostgreSQLContainer;

/**
 * Test that PostgreSQL RETURNING clause works through the ZTD shadow store.
 *
 * Finding: RETURNING clause silently drops the result set on PostgreSQL.
 * Mutations execute correctly (shadow store is updated), but the RETURNING
 * result always returns 0 rows. This is a silent data loss bug — users
 * relying on RETURNING to get inserted/updated/deleted row data will get
 * no data and no error.
 */
class PostgresReturningClauseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE items ('
            . '  id SERIAL PRIMARY KEY,'
            . '  name TEXT,'
            . '  price NUMERIC(10,2),'
            . '  active INT DEFAULT 1'
            . ')',
        ];
    }

    protected function getTableNames(): array
    {
        return ['items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO items (id, name, price, active) VALUES (1, 'Widget', 9.99, 1)");
        $this->pdo->exec("INSERT INTO items (id, name, price, active) VALUES (2, 'Gadget', 24.99, 1)");
        $this->pdo->exec("INSERT INTO items (id, name, price, active) VALUES (3, 'Doohickey', 4.50, 1)");
        $this->pdo->exec("INSERT INTO items (id, name, price, active) VALUES (4, 'Thingamajig', 14.75, 0)");
    }

    /**
     * INSERT ... RETURNING * silently returns 0 rows.
     * The INSERT itself succeeds (shadow store is updated).
     */
    public function testInsertReturningAllReturnsEmpty(): void
    {
        $stmt = $this->pdo->query(
            "INSERT INTO items (id, name, price, active) VALUES (5, 'Sprocket', 7.25, 1) RETURNING *"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // BUG: RETURNING returns 0 rows
        $this->assertCount(0, $rows, 'INSERT RETURNING * returns 0 rows (expected 1)');

        // But the INSERT itself succeeded in the shadow store
        $verify = $this->ztdQuery("SELECT name FROM items WHERE id = 5");
        $this->assertCount(1, $verify);
        $this->assertSame('Sprocket', $verify[0]['name']);
    }

    /**
     * INSERT ... RETURNING specific columns also returns 0 rows.
     */
    public function testInsertReturningSpecificColumnsReturnsEmpty(): void
    {
        $stmt = $this->pdo->query(
            "INSERT INTO items (id, name, price, active) VALUES (6, 'Gizmo', 19.50, 1) RETURNING id, name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'INSERT RETURNING id, name returns 0 rows (expected 1)');
    }

    /**
     * UPDATE ... RETURNING * silently returns 0 rows.
     * The UPDATE itself succeeds.
     */
    public function testUpdateReturningAllReturnsEmpty(): void
    {
        $stmt = $this->pdo->query(
            "UPDATE items SET price = 29.99 WHERE name = 'Gadget' RETURNING *"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'UPDATE RETURNING * returns 0 rows (expected 1)');

        // But the UPDATE itself succeeded
        $verify = $this->ztdQuery("SELECT price FROM items WHERE name = 'Gadget'");
        $this->assertSame('29.99', $verify[0]['price']);
    }

    /**
     * DELETE ... RETURNING * silently returns 0 rows.
     * The DELETE itself succeeds.
     */
    public function testDeleteReturningAllReturnsEmpty(): void
    {
        $stmt = $this->pdo->query(
            "DELETE FROM items WHERE name = 'Doohickey' RETURNING *"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'DELETE RETURNING * returns 0 rows (expected 1)');

        // But the DELETE itself succeeded
        $remaining = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM items');
        $this->assertSame(3, (int) $remaining[0]['cnt']);
    }

    /**
     * Prepared INSERT ... RETURNING also returns 0 rows.
     */
    public function testInsertReturningWithPreparedStatementReturnsEmpty(): void
    {
        $stmt = $this->pdo->prepare(
            'INSERT INTO items (id, name, price, active) VALUES (?, ?, ?, ?) RETURNING id, name, price'
        );
        $stmt->execute([7, 'Contraption', 33.33, 1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'Prepared INSERT RETURNING returns 0 rows (expected 1)');
    }

    /**
     * Physical isolation — mutations via RETURNING reach shadow but not physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $ztdRows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM items');
        $this->assertSame(4, (int) $ztdRows[0]['cnt'], 'Shadow store has 4 items');

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $physicalRows = $raw->query('SELECT COUNT(*) AS cnt FROM items')->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(0, (int) $physicalRows[0]['cnt'],
            'Physical table must not contain shadow-inserted data');
    }
}
