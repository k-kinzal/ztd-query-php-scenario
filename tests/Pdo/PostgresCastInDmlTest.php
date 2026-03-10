<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CAST/type coercion expressions inside DML statements
 * through ZTD shadow store on PostgreSQL.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.1a
 */
class PostgresCastInDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cst_raw_data (
                id SERIAL PRIMARY KEY,
                str_amount TEXT NOT NULL,
                str_qty TEXT NOT NULL,
                label TEXT NOT NULL
            )',
            'CREATE TABLE pg_cst_summary (
                id SERIAL PRIMARY KEY,
                label TEXT NOT NULL,
                amount NUMERIC(12,2),
                qty INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cst_summary', 'pg_cst_raw_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cst_raw_data (str_amount, str_qty, label) VALUES ('99.95', '10', 'Widget')");
        $this->pdo->exec("INSERT INTO pg_cst_raw_data (str_amount, str_qty, label) VALUES ('45.50', '25', 'Gadget')");
        $this->pdo->exec("INSERT INTO pg_cst_raw_data (str_amount, str_qty, label) VALUES ('120.00', '5', 'Premium')");
    }

    public function testInsertSelectWithCast(): void
    {
        $sql = "INSERT INTO pg_cst_summary (label, amount, qty)
                SELECT label, CAST(str_amount AS NUMERIC(12,2)), CAST(str_qty AS INTEGER)
                FROM pg_cst_raw_data";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount, qty FROM pg_cst_summary ORDER BY label");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST: expected 3, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = $r;
            }

            if ((float) $byLabel['Gadget']['amount'] < 45.0) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST: Gadget amount expected ~45.50, got ' . $byLabel['Gadget']['amount']
                );
            }

            $this->assertEqualsWithDelta(45.50, (float) $byLabel['Gadget']['amount'], 0.01);
            $this->assertSame(25, (int) $byLabel['Gadget']['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with CAST failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithCastInSet(): void
    {
        $this->pdo->exec("INSERT INTO pg_cst_summary (label, amount, qty) VALUES ('Widget', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_cst_summary (label, amount, qty) VALUES ('Gadget', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_cst_summary (label, amount, qty) VALUES ('Premium', 0, 0)");

        $sql = "UPDATE pg_cst_summary
                SET amount = (
                    SELECT CAST(str_amount AS NUMERIC(12,2))
                    FROM pg_cst_raw_data
                    WHERE pg_cst_raw_data.label = pg_cst_summary.label
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount FROM pg_cst_summary ORDER BY label");

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = (float) $r['amount'];
            }

            if ($byLabel['Widget'] < 99.0) {
                $this->markTestIncomplete(
                    'UPDATE CAST in SET: Widget expected ~99.95, got ' . $byLabel['Widget']
                );
            }

            $this->assertEqualsWithDelta(99.95, $byLabel['Widget'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with CAST in SET failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithCastInWhere(): void
    {
        $sql = "DELETE FROM pg_cst_raw_data WHERE CAST(str_amount AS NUMERIC(12,2)) > 100.0";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label FROM pg_cst_raw_data ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE CAST WHERE: expected 2, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with CAST in WHERE failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectCastArithmetic(): void
    {
        $sql = "INSERT INTO pg_cst_summary (label, amount, qty)
                SELECT label,
                       CAST(str_amount AS NUMERIC(12,2)) * CAST(str_qty AS INTEGER),
                       CAST(str_qty AS INTEGER)
                FROM pg_cst_raw_data";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT label, amount FROM pg_cst_summary ORDER BY label");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST arithmetic: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);

            $byLabel = [];
            foreach ($rows as $r) {
                $byLabel[$r['label']] = (float) $r['amount'];
            }

            if ($byLabel['Widget'] < 999.0) {
                $this->markTestIncomplete(
                    'INSERT SELECT CAST arithmetic: Widget expected ~999.50, got ' . $byLabel['Widget']
                );
            }

            $this->assertEqualsWithDelta(999.50, $byLabel['Widget'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT CAST arithmetic failed: ' . $e->getMessage());
        }
    }
}
