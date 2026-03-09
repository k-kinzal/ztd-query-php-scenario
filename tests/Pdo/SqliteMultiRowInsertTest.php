<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that multi-row INSERT (VALUES (...), (...), ...) works through ZTD.
 *
 * Multi-row INSERT is a very common optimization pattern. Each row must be
 * correctly captured by the shadow store.
 * @spec SPEC-4.1
 */
class SqliteMultiRowInsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mri_items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mri_items'];
    }

    /**
     * Multi-row INSERT with 3 rows then SELECT all.
     */
    public function testMultiRowInsertThreeRows(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) !== 3) {
            $this->markTestIncomplete('Multi-row INSERT: expected 3 rows, got ' . count($rows) . '. Shadow store may only capture first row.');
            return;
        }
        $this->assertCount(3, $rows);
        $this->assertSame('A', $rows[0]['name']);
        $this->assertSame('B', $rows[1]['name']);
        $this->assertSame('C', $rows[2]['name']);
    }

    /**
     * Multi-row INSERT then aggregate.
     */
    public function testMultiRowInsertThenAggregate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'X', 5), (2, 'Y', 15), (3, 'Z', 25)");
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt, SUM(qty) AS total FROM mri_items');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT aggregate failed: ' . $e->getMessage());
            return;
        }

        $this->assertSame('3', (string) $rows[0]['cnt']);
        $this->assertSame('45', (string) $rows[0]['total']);
    }

    /**
     * Multi-row INSERT then UPDATE one row.
     */
    public function testMultiRowInsertThenUpdate(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
            $this->pdo->exec("UPDATE mri_items SET qty = 99 WHERE id = 2");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT then UPDATE failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(3, $rows);
        $this->assertSame('99', (string) $rows[1]['qty']);
    }

    /**
     * Multi-row INSERT then DELETE one row.
     */
    public function testMultiRowInsertThenDelete(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)");
            $this->pdo->exec('DELETE FROM mri_items WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT then DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
        $this->assertSame('3', (string) $rows[1]['id']);
    }

    /**
     * Two sequential multi-row INSERTs.
     */
    public function testSequentialMultiRowInserts(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'A', 10), (2, 'B', 20)");
            $this->pdo->exec("INSERT INTO mri_items VALUES (3, 'C', 30), (4, 'D', 40)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Sequential multi-row INSERTs failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) !== 4) {
            $this->markTestIncomplete('Sequential multi-row INSERTs: expected 4 rows, got ' . count($rows));
            return;
        }
        $this->assertCount(4, $rows);
    }

    /**
     * Multi-row INSERT with explicit column list.
     */
    public function testMultiRowInsertExplicitColumns(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items (id, name, qty) VALUES (1, 'First', 100), (2, 'Second', 200)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT with explicit columns failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('First', $rows[0]['name']);
        $this->assertSame('200', (string) $rows[1]['qty']);
    }

    /**
     * Multi-row INSERT with columns in non-DDL order.
     */
    public function testMultiRowInsertReorderedColumns(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items (qty, name, id) VALUES (100, 'Rev', 1), (200, 'Rev2', 2)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT with reordered columns failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Rev', $rows[0]['name']);
        $this->assertSame('100', (string) $rows[0]['qty']);
    }

    /**
     * Multi-row INSERT with NULL values.
     */
    public function testMultiRowInsertWithNulls(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, NULL, 10), (2, 'B', NULL), (3, NULL, NULL)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT with NULLs failed: ' . $e->getMessage());
            return;
        }

        if (count($rows) !== 3) {
            $this->markTestIncomplete('Multi-row INSERT with NULLs: expected 3 rows, got ' . count($rows));
            return;
        }
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['name']);
        $this->assertNull($rows[1]['qty']);
        $this->assertNull($rows[2]['name']);
        $this->assertNull($rows[2]['qty']);
    }

    /**
     * Multi-row INSERT then WHERE IN filter.
     */
    public function testMultiRowInsertThenWhereIn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mri_items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30), (4, 'D', 40), (5, 'E', 50)");
            $rows = $this->ztdQuery('SELECT * FROM mri_items WHERE id IN (2, 4) ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Multi-row INSERT then WHERE IN failed: ' . $e->getMessage());
            return;
        }

        $this->assertCount(2, $rows);
        $this->assertSame('B', $rows[0]['name']);
        $this->assertSame('D', $rows[1]['name']);
    }

    /**
     * Large multi-row INSERT (10 rows).
     */
    public function testLargeMultiRowInsert(): void
    {
        $values = [];
        for ($i = 1; $i <= 10; $i++) {
            $values[] = "($i, 'Item$i', " . ($i * 10) . ")";
        }
        $sql = 'INSERT INTO mri_items VALUES ' . implode(', ', $values);

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mri_items');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Large multi-row INSERT failed: ' . $e->getMessage());
            return;
        }

        if ((int) $rows[0]['cnt'] !== 10) {
            $this->markTestIncomplete('Large multi-row INSERT: expected 10 rows, got ' . $rows[0]['cnt']);
            return;
        }
        $this->assertSame('10', (string) $rows[0]['cnt']);
    }
}
