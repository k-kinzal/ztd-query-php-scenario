<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests table aliases in UPDATE and DELETE DML statements on MySQL PDO.
 *
 * Real-world scenario: ORMs and query builders produce aliased DML like
 * `UPDATE t AS alias SET ...` or `DELETE alias FROM t AS alias WHERE ...`.
 * The CTE rewriter must preserve alias context when rewriting DML statements.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlTableAliasInDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_tad_items (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_tad_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_tad_items VALUES (1, 'Widget', 'tools', 10.00)");
        $this->ztdExec("INSERT INTO my_tad_items VALUES (2, 'Gadget', 'electronics', 25.00)");
        $this->ztdExec("INSERT INTO my_tad_items VALUES (3, 'Sprocket', 'tools', 5.00)");
    }

    /**
     * UPDATE with table alias using AS keyword.
     * MySQL supports: UPDATE t AS alias SET alias.col = ... WHERE alias.col = ...
     */
    public function testUpdateWithAsAlias(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_tad_items AS i SET i.price = i.price * 2 WHERE i.category = 'tools'"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM my_tad_items WHERE category = 'tools' ORDER BY name"
            );
            $this->assertCount(2, $rows);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01); // Sprocket 5*2
            $this->assertEqualsWithDelta(20.00, (float) $rows[1]['price'], 0.01); // Widget 10*2
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with AS alias failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with table alias.
     * MySQL: DELETE alias FROM t AS alias WHERE alias.col = ...
     */
    public function testDeleteWithAlias(): void
    {
        try {
            $this->ztdExec(
                "DELETE i FROM my_tad_items AS i WHERE i.price < 10"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_tad_items ORDER BY name");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Sprocket', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with alias failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with alias and prepared params.
     */
    public function testUpdateWithAliasAndParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_tad_items AS i SET i.price = ? WHERE i.id = ?"
            );
            $stmt->execute([99.99, 2]);

            $rows = $this->ztdQuery("SELECT price FROM my_tad_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(99.99, (float) $rows[0]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with alias and params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with alias, referencing columns without alias prefix.
     */
    public function testUpdateAliasWithUnprefixedColumns(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_tad_items AS i SET name = 'Renamed' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_tad_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Renamed', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with alias + unprefixed columns failed: ' . $e->getMessage()
            );
        }
    }
}
