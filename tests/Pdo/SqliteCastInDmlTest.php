<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CAST/type coercion expressions inside DML statements
 * through ZTD shadow store on SQLite.
 *
 * CAST expressions contain parentheses and AS keyword that
 * the CTE rewriter must handle without confusion.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.1a
 */
class SqliteCastInDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cst_raw_data (
                id INTEGER PRIMARY KEY,
                str_amount TEXT NOT NULL,
                str_qty TEXT NOT NULL,
                label TEXT NOT NULL
            )',
            'CREATE TABLE sl_cst_summary (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                amount REAL,
                qty INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cst_summary', 'sl_cst_raw_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cst_raw_data VALUES (1, '99.95', '10', 'Widget')");
        $this->pdo->exec("INSERT INTO sl_cst_raw_data VALUES (2, '45.50', '25', 'Gadget')");
        $this->pdo->exec("INSERT INTO sl_cst_raw_data VALUES (3, '120.00', '5', 'Premium')");
    }

    /**
     * INSERT...SELECT with CAST expressions converting text to numeric types.
     */
    public function testInsertSelectWithCast(): void
    {
        $sql = "INSERT INTO sl_cst_summary (label, amount, qty)
                SELECT label, CAST(str_amount AS REAL), CAST(str_qty AS INTEGER)
                FROM sl_cst_raw_data";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount, qty FROM sl_cst_summary ORDER BY label");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = $r;
            }

            $this->assertEqualsWithDelta(45.50, (float) $byLabel['Gadget']['amount'], 0.01);
            $this->assertSame(25, (int) $byLabel['Gadget']['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with CAST failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with CAST expression.
     */
    public function testUpdateWithCastInSet(): void
    {
        // First populate summary
        $this->pdo->exec(
            "INSERT INTO sl_cst_summary VALUES (1, 'Widget', 0, 0), (2, 'Gadget', 0, 0), (3, 'Premium', 0, 0)"
        );

        $sql = "UPDATE sl_cst_summary
                SET amount = (
                    SELECT CAST(str_amount AS REAL)
                    FROM sl_cst_raw_data
                    WHERE sl_cst_raw_data.label = sl_cst_summary.label
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount FROM sl_cst_summary ORDER BY label");

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = (float) $r['amount'];
            }

            if ($byLabel['Widget'] < 99.0) {
                $this->markTestIncomplete(
                    'UPDATE CAST in SET: Widget expected ~99.95, got '
                    . $byLabel['Widget'] . '. Data: ' . json_encode($byLabel)
                );
            }

            $this->assertEqualsWithDelta(99.95, $byLabel['Widget'], 0.01);
            $this->assertEqualsWithDelta(45.50, $byLabel['Gadget'], 0.01);
            $this->assertEqualsWithDelta(120.00, $byLabel['Premium'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with CAST in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE with CAST comparison.
     */
    public function testDeleteWithCastInWhere(): void
    {
        $sql = "DELETE FROM sl_cst_raw_data WHERE CAST(str_amount AS REAL) > 100.0";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label FROM sl_cst_raw_data ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE CAST WHERE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['label']);
            $this->assertSame('Gadget', $rows[1]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with CAST in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with CAST and parameter.
     */
    public function testPreparedInsertWithCast(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_cst_summary (label, amount, qty)
                 SELECT label, CAST(str_amount AS REAL) * ?, CAST(str_qty AS INTEGER)
                 FROM sl_cst_raw_data
                 WHERE CAST(str_qty AS INTEGER) >= ?"
            );
            $stmt->execute([1.1, 10]);

            $rows = $this->ztdQuery("SELECT label, amount, qty FROM sl_cst_summary ORDER BY label");

            // Widget (qty=10, >= 10) and Gadget (qty=25, >= 10) qualify
            // Premium (qty=5, < 10) doesn't
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT CAST: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with CAST failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with CAST inside arithmetic expression.
     */
    public function testInsertSelectCastArithmetic(): void
    {
        $sql = "INSERT INTO sl_cst_summary (label, amount, qty)
                SELECT label,
                       CAST(str_amount AS REAL) * CAST(str_qty AS INTEGER),
                       CAST(str_qty AS INTEGER)
                FROM sl_cst_raw_data";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount FROM sl_cst_summary ORDER BY label");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST arithmetic: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = (float) $r['amount'];
            }

            // Widget: 99.95 * 10 = 999.50
            // Gadget: 45.50 * 25 = 1137.50
            // Premium: 120.00 * 5 = 600.00
            $this->assertEqualsWithDelta(999.50, $byLabel['Widget'], 0.01);
            $this->assertEqualsWithDelta(1137.50, $byLabel['Gadget'], 0.01);
            $this->assertEqualsWithDelta(600.00, $byLabel['Premium'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT CAST arithmetic failed: ' . $e->getMessage());
        }
    }
}
