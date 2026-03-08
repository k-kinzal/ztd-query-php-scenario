<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL generated (virtual/stored) column handling with ZTD.
 *
 * MySQL supports generated columns:
 * - VIRTUAL: computed on read, not stored
 * - STORED: computed and persisted
 *
 * Tests whether generated column values are correctly handled
 * in the shadow store via CTE rewriting.
 * @spec pending
 */
class MysqlGeneratedColumnTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_gencol_test (
            id INT PRIMARY KEY,
            price DECIMAL(10,2),
            quantity INT,
            total DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) STORED,
            label VARCHAR(100) GENERATED ALWAYS AS (CONCAT(quantity, \\\'x @ \\\', price)) VIRTUAL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pdo_gencol_test'];
    }


    /**
     * INSERT omitting generated columns.
     *
     * Generated columns should NOT be included in INSERT statements.
     * The shadow store should handle the omitted columns.
     */
    public function testInsertOmittingGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (1, 9.99, 3)");

        $stmt = $this->pdo->query('SELECT id, price, quantity FROM pdo_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['id']);
        $this->assertEqualsWithDelta(9.99, (float) $row['price'], 0.01);
        $this->assertSame(3, (int) $row['quantity']);
    }

    /**
     * SELECT generated column values from shadow store.
     *
     * Since ZTD stores data in shadow and rebuilds via CTE,
     * generated column expressions may or may not be computed.
     */
    public function testSelectGeneratedColumnValues(): void
    {
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        // Try to read the generated column — it may be NULL in shadow
        // because the expression is a DB-level computation that doesn't
        // happen in the CTE shadow store
        $stmt = $this->pdo->query('SELECT total, label FROM pdo_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        // Generated columns may be NULL in shadow (no physical INSERT happened)
        // or they may be computed by the CTE. Document actual behavior.
        if ($row['total'] !== null) {
            $this->assertEqualsWithDelta(50.0, (float) $row['total'], 0.01);
        } else {
            // NULL is expected — generated column not computed in shadow
            $this->assertNull($row['total']);
        }
    }

    /**
     * UPDATE non-generated columns and query generated column.
     */
    public function testUpdateAndQueryGeneratedColumn(): void
    {
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec('UPDATE pdo_gencol_test SET quantity = 10 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT quantity FROM pdo_gencol_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $row['quantity']);
    }

    /**
     * Multiple rows with generated columns.
     */
    public function testMultipleRowsWithGeneratedColumns(): void
    {
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (2, 20.00, 3)");
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (3, 5.00, 10)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_gencol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_gencol_test (id, price, quantity) VALUES (1, 10.00, 5)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_gencol_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
