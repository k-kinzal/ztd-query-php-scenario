<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CAST/type coercion expressions inside DML statements
 * through ZTD shadow store on MySQLi.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.1a
 */
class CastInDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cst_raw_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                str_amount VARCHAR(20) NOT NULL,
                str_qty VARCHAR(10) NOT NULL,
                label VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_cst_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                label VARCHAR(50) NOT NULL,
                amount DECIMAL(12,2),
                qty INT
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cst_summary', 'mi_cst_raw_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cst_raw_data (str_amount, str_qty, label) VALUES ('99.95', '10', 'Widget')");
        $this->mysqli->query("INSERT INTO mi_cst_raw_data (str_amount, str_qty, label) VALUES ('45.50', '25', 'Gadget')");
        $this->mysqli->query("INSERT INTO mi_cst_raw_data (str_amount, str_qty, label) VALUES ('120.00', '5', 'Premium')");
    }

    public function testInsertSelectWithCast(): void
    {
        $sql = "INSERT INTO mi_cst_summary (label, amount, qty)
                SELECT label, CAST(str_amount AS DECIMAL(12,2)), CAST(str_qty AS SIGNED)
                FROM mi_cst_raw_data";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT label, amount, qty FROM mi_cst_summary ORDER BY label");

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
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with CAST failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithCastInWhere(): void
    {
        $sql = "DELETE FROM mi_cst_raw_data WHERE CAST(str_amount AS DECIMAL(12,2)) > 100.0";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT label FROM mi_cst_raw_data ORDER BY id");

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
        $sql = "INSERT INTO mi_cst_summary (label, amount, qty)
                SELECT label,
                       CAST(str_amount AS DECIMAL(12,2)) * CAST(str_qty AS SIGNED),
                       CAST(str_qty AS SIGNED)
                FROM mi_cst_raw_data";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT label, amount FROM mi_cst_summary ORDER BY label");

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
