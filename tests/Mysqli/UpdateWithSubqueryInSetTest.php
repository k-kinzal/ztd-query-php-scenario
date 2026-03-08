<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with subquery in SET clause via MySQLi.
 *
 * Cross-platform parity with MysqlUpdateWithSubqueryInSetTest (PDO).
 * @spec SPEC-4.2
 */
class UpdateWithSubqueryInSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_usub_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE mi_usub_prices (id INT PRIMARY KEY, product_id INT, new_price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_usub_products', 'mi_usub_prices'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_usub_products VALUES (1, 'Widget', 10.00)");
        $this->mysqli->query("INSERT INTO mi_usub_products VALUES (2, 'Gadget', 20.00)");
        $this->mysqli->query("INSERT INTO mi_usub_products VALUES (3, 'Doohickey', 30.00)");
        $this->mysqli->query('INSERT INTO mi_usub_prices VALUES (1, 1, 15.00)');
        $this->mysqli->query('INSERT INTO mi_usub_prices VALUES (2, 2, 25.00)');
    }

    /**
     * Non-correlated scalar subquery in SET.
     */
    public function testNonCorrelatedScalarSubquery(): void
    {
        $this->mysqli->query('UPDATE mi_usub_products SET price = (SELECT MAX(new_price) FROM mi_usub_prices) WHERE id = 3');

        $result = $this->mysqli->query('SELECT price FROM mi_usub_products WHERE id = 3');
        $this->assertEqualsWithDelta(25.00, (float) $result->fetch_assoc()['price'], 0.01);
    }

    /**
     * Self-referencing non-correlated subquery in SET.
     */
    public function testSelfReferencingNonCorrelatedSubquery(): void
    {
        $this->mysqli->query('UPDATE mi_usub_products SET price = (SELECT MAX(price) FROM mi_usub_products) WHERE id = 1');

        $result = $this->mysqli->query('SELECT price FROM mi_usub_products WHERE id = 1');
        $this->assertEqualsWithDelta(30.00, (float) $result->fetch_assoc()['price'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('UPDATE mi_usub_products SET price = (SELECT MAX(new_price) FROM mi_usub_prices) WHERE id = 1');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_usub_products');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
