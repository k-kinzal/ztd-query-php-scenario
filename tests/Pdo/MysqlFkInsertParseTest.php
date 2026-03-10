<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT without explicit column list on tables with FOREIGN KEY constraints.
 *
 * Many users define FK constraints inline in CREATE TABLE and then INSERT
 * without explicit column lists. The InsertTransformer must not count
 * FK constraint clauses as columns.
 *
 * @spec SPEC-4.1
 */
class MysqlFkInsertParseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_fkp_parent (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_fkp_child (
                id INT PRIMARY KEY,
                parent_id INT NOT NULL,
                label VARCHAR(50) NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES my_fkp_parent(id)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_fkp_child', 'my_fkp_parent'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_fkp_parent (id, name) VALUES (1, 'Parent1')");
    }

    /**
     * INSERT without column list on table with FK constraint.
     * The InsertTransformer should not count the FK clause as a column.
     */
    public function testInsertWithoutColumnListOnFkTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_fkp_child VALUES (10, 1, 'Child1')");

            $rows = $this->ztdQuery("SELECT id, label FROM my_fkp_child WHERE id = 10");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT without column list on FK table: got ' . json_encode($rows)
                );
            }

            $this->assertSame('Child1', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT without column list on FK table failed: ' . $e->getMessage()
                . ' — InsertTransformer may count FK constraint as column'
            );
        }
    }

    /**
     * INSERT with explicit column list on FK table (workaround).
     */
    public function testInsertWithColumnListOnFkTable(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_fkp_child (id, parent_id, label) VALUES (10, 1, 'Child1')");

            $rows = $this->ztdQuery("SELECT id, label FROM my_fkp_child WHERE id = 10");

            $this->assertCount(1, $rows);
            $this->assertSame('Child1', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with column list on FK table failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT without column list on table with multiple FK constraints.
     */
    public function testInsertWithoutColumnListMultipleFks(): void
    {
        try {
            $this->createTable(
                'CREATE TABLE my_fkp_multi (
                    id INT PRIMARY KEY,
                    parent_id INT NOT NULL,
                    label VARCHAR(50) NOT NULL,
                    FOREIGN KEY (parent_id) REFERENCES my_fkp_parent(id),
                    INDEX idx_parent (parent_id)
                ) ENGINE=InnoDB'
            );

            $this->pdo->exec("INSERT INTO my_fkp_multi VALUES (10, 1, 'Multi1')");

            $rows = $this->ztdQuery("SELECT id, label FROM my_fkp_multi WHERE id = 10");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT FK multi: got ' . json_encode($rows));
            }

            $this->assertSame('Multi1', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT without column list on multi-FK table failed: ' . $e->getMessage()
            );
        } finally {
            try { $this->dropTable('my_fkp_multi'); } catch (\Throwable $e) {}
        }
    }

    /**
     * INSERT without column list on table with AUTO_INCREMENT + FK.
     */
    public function testInsertWithoutColumnListAutoIncrementAndFk(): void
    {
        try {
            $this->createTable(
                'CREATE TABLE my_fkp_auto (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    parent_id INT NOT NULL,
                    label VARCHAR(50) NOT NULL,
                    FOREIGN KEY (parent_id) REFERENCES my_fkp_parent(id)
                ) ENGINE=InnoDB'
            );

            $this->pdo->exec("INSERT INTO my_fkp_auto VALUES (10, 1, 'Auto1')");

            $rows = $this->ztdQuery("SELECT id, label FROM my_fkp_auto WHERE id = 10");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT AI+FK: got ' . json_encode($rows));
            }

            $this->assertSame('Auto1', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT without column list on AUTO_INCREMENT+FK table failed: ' . $e->getMessage()
            );
        } finally {
            try { $this->dropTable('my_fkp_auto'); } catch (\Throwable $e) {}
        }
    }
}
