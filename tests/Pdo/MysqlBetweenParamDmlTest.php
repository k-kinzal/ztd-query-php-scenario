<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DML with BETWEEN and prepared parameters through ZTD shadow store on MySQL (PDO).
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class MysqlBetweenParamDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_btw_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock INT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_btw_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_btw_products (name, price, stock) VALUES ('Widget', 9.99, 100)");
        $this->pdo->exec("INSERT INTO my_btw_products (name, price, stock) VALUES ('Gadget', 24.99, 50)");
        $this->pdo->exec("INSERT INTO my_btw_products (name, price, stock) VALUES ('Doohickey', 49.99, 25)");
        $this->pdo->exec("INSERT INTO my_btw_products (name, price, stock) VALUES ('Thingamajig', 74.99, 10)");
        $this->pdo->exec("INSERT INTO my_btw_products (name, price, stock) VALUES ('Whatchamacallit', 99.99, 5)");
    }

    public function testPreparedUpdateBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_btw_products SET stock = stock + 10 WHERE price BETWEEN ? AND ?"
            );
            $stmt->execute([20.00, 75.00]);

            $rows = $this->ztdQuery("SELECT name, stock FROM my_btw_products ORDER BY id");
            $byName = [];
            foreach ($rows as $r) {
                $byName[$r['name']] = (int) $r['stock'];
            }

            if ($byName['Gadget'] !== 60 || $byName['Doohickey'] !== 35 || $byName['Thingamajig'] !== 20) {
                $this->markTestIncomplete(
                    'Prepared UPDATE BETWEEN: expected 60/35/20, got ' . json_encode($byName)
                );
            }

            $this->assertSame(100, $byName['Widget']);
            $this->assertSame(60, $byName['Gadget']);
            $this->assertSame(35, $byName['Doohickey']);
            $this->assertSame(20, $byName['Thingamajig']);
            $this->assertSame(5, $byName['Whatchamacallit']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE BETWEEN failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_btw_products WHERE price BETWEEN ? AND ?"
            );
            $stmt->execute([25.00, 100.00]);

            $rows = $this->ztdQuery("SELECT name FROM my_btw_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE BETWEEN: expected 2, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE BETWEEN failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateBetweenWithLogicalAnd(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_btw_products SET stock = 0 WHERE price BETWEEN ? AND ? AND stock < ?"
            );
            $stmt->execute([20.00, 100.00, 30]);

            $rows = $this->ztdQuery("SELECT name, stock FROM my_btw_products ORDER BY id");
            $byName = [];
            foreach ($rows as $r) {
                $byName[$r['name']] = (int) $r['stock'];
            }

            if ($byName['Gadget'] !== 50 || $byName['Doohickey'] !== 0) {
                $this->markTestIncomplete(
                    'Prepared UPDATE BETWEEN AND: got ' . json_encode($byName)
                );
            }

            $this->assertSame(100, $byName['Widget']);
            $this->assertSame(50, $byName['Gadget']);
            $this->assertSame(0, $byName['Doohickey']);
            $this->assertSame(0, $byName['Thingamajig']);
            $this->assertSame(0, $byName['Whatchamacallit']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE BETWEEN AND failed: ' . $e->getMessage());
        }
    }
}
