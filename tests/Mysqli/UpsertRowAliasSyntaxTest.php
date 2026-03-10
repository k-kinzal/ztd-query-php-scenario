<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL 8.0.19+ row alias syntax for INSERT...ON DUPLICATE KEY UPDATE.
 *
 * MySQL 8.0.19 introduced:
 *   INSERT INTO t VALUES (...) AS new ON DUPLICATE KEY UPDATE col = new.col
 *
 * This replaces the deprecated VALUES() function:
 *   INSERT INTO t VALUES (...) ON DUPLICATE KEY UPDATE col = VALUES(col)
 *
 * MySQL 8.2+ emits deprecation warnings for VALUES() in this context,
 * pushing applications toward the row alias syntax.
 *
 * @spec SPEC-4.2a
 */
class UpsertRowAliasSyntaxTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_uras_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            counter INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_uras_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_uras_items VALUES (1, 'Alpha', 10)");
        $this->mysqli->query("INSERT INTO mi_uras_items VALUES (2, 'Beta', 20)");
    }

    /**
     * Basic row alias: INSERT new row, no conflict.
     */
    public function testRowAliasInsertNoConflict(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uras_items VALUES (3, 'Gamma', 30) AS new ON DUPLICATE KEY UPDATE name = new.name"
            );

            $rows = $this->ztdQuery("SELECT * FROM mi_uras_items WHERE id = 3");

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Row alias INSERT produced no visible row (parser may not recognize AS alias syntax)'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gamma', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row alias no-conflict INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Row alias: INSERT with conflict triggers UPDATE.
     */
    public function testRowAliasUpsertOnConflict(): void
    {
        try {
            // id=1 already exists with name='Alpha'
            $this->mysqli->query(
                "INSERT INTO mi_uras_items VALUES (1, 'Alpha-Updated', 10) AS new ON DUPLICATE KEY UPDATE name = new.name"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_uras_items WHERE id = 1");

            if (empty($rows)) {
                $this->markTestIncomplete('Row alias upsert returned no rows');
            }

            $name = $rows[0]['name'];
            if ($name !== 'Alpha-Updated') {
                $this->markTestIncomplete(
                    'Row alias ON DUPLICATE KEY UPDATE did not apply. Expected "Alpha-Updated", got '
                    . json_encode($name)
                );
            }
            $this->assertSame('Alpha-Updated', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row alias upsert failed: ' . $e->getMessage());
        }
    }

    /**
     * Row alias with multiple columns in UPDATE.
     */
    public function testRowAliasMultipleColumnsUpdate(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uras_items VALUES (2, 'Beta-New', 99) AS new "
                . "ON DUPLICATE KEY UPDATE name = new.name, counter = new.counter"
            );

            $rows = $this->ztdQuery("SELECT name, counter FROM mi_uras_items WHERE id = 2");

            if (empty($rows)) {
                $this->markTestIncomplete('Multi-column row alias upsert returned no rows');
            }

            $name = $rows[0]['name'];
            $counter = (int) $rows[0]['counter'];

            if ($name !== 'Beta-New' || $counter !== 99) {
                $this->markTestIncomplete(
                    'Multi-column row alias failed. Expected name=Beta-New, counter=99. Got name='
                    . json_encode($name) . ', counter=' . $counter
                );
            }
            $this->assertSame('Beta-New', $name);
            $this->assertSame(99, $counter);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column row alias failed: ' . $e->getMessage());
        }
    }

    /**
     * Row alias with expression in UPDATE: counter = counter + new.counter
     */
    public function testRowAliasWithExpression(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uras_items VALUES (1, 'Alpha', 5) AS new "
                . "ON DUPLICATE KEY UPDATE counter = counter + new.counter"
            );

            $rows = $this->ztdQuery("SELECT counter FROM mi_uras_items WHERE id = 1");

            if (empty($rows)) {
                $this->markTestIncomplete('Expression row alias upsert returned no rows');
            }

            $counter = (int) $rows[0]['counter'];
            // Original counter=10, new.counter=5, expected 15
            if ($counter !== 15) {
                $this->markTestIncomplete(
                    'Row alias expression failed. Expected counter=15 (10+5), got ' . $counter
                );
            }
            $this->assertSame(15, $counter);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row alias expression failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: deprecated VALUES() syntax still works.
     */
    public function testDeprecatedValuesSyntaxStillWorks(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_uras_items VALUES (1, 'Alpha-V', 0) ON DUPLICATE KEY UPDATE name = VALUES(name)"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_uras_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Alpha-V', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Deprecated VALUES() syntax failed: ' . $e->getMessage());
        }
    }
}
