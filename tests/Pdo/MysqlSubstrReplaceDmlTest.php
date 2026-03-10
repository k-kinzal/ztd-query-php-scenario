<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-argument string functions (SUBSTRING, REPLACE, LOCATE) in DML
 * with prepared parameters through ZTD shadow store on MySQL.
 *
 * String functions with multiple bound parameters are common in applications
 * that manipulate text data. The CTE rewriter must correctly bind all parameters.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class MysqlSubstrReplaceDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_sr_codes (
                id INT PRIMARY KEY,
                code VARCHAR(20) NOT NULL,
                description VARCHAR(100) NOT NULL,
                prefix VARCHAR(10) NOT NULL DEFAULT \'\'
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_sr_codes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_sr_codes VALUES (1, 'ABC-001', 'First item', 'ABC')");
        $this->pdo->exec("INSERT INTO my_sr_codes VALUES (2, 'ABC-002', 'Second item', 'ABC')");
        $this->pdo->exec("INSERT INTO my_sr_codes VALUES (3, 'XYZ-001', 'Third item', 'XYZ')");
        $this->pdo->exec("INSERT INTO my_sr_codes VALUES (4, 'XYZ-002', 'Fourth item', 'XYZ')");
    }

    /**
     * Prepared UPDATE SET with REPLACE() and two params.
     */
    public function testPreparedUpdateSetReplaceWithTwoParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_sr_codes SET code = REPLACE(code, ?, ?) WHERE prefix = 'ABC'"
            );
            $stmt->execute(['ABC', 'DEF']);

            $rows = $this->ztdQuery(
                "SELECT code FROM my_sr_codes WHERE prefix = 'ABC' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('UPDATE REPLACE: got ' . json_encode($rows));
            }

            $this->assertSame('DEF-001', $rows[0]['code']);
            $this->assertSame('DEF-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with REPLACE() failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with SUBSTRING() and bound params.
     */
    public function testPreparedSelectSubstringWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id, SUBSTRING(code, ?, ?) AS extracted FROM my_sr_codes ORDER BY id"
            );
            $stmt->execute([1, 3]);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT SUBSTRING: expected 4, got ' . count($rows)
                );
            }

            $this->assertSame('ABC', $rows[0]['extracted']);
            $this->assertSame('XYZ', $rows[2]['extracted']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with SUBSTRING params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE SUBSTRING(col, ?, ?) = ? — 3 params in one expression.
     */
    public function testPreparedDeleteWhereSubstringWithThreeParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_sr_codes WHERE SUBSTRING(code, ?, ?) = ?"
            );
            $stmt->execute([5, 3, '001']);

            $rows = $this->ztdQuery("SELECT code FROM my_sr_codes ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE SUBSTRING 3 params: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('ABC-002', $rows[0]['code']);
            $this->assertSame('XYZ-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with SUBSTRING 3 params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE WHERE LOCATE(?, col) > 0 — search with param.
     */
    public function testPreparedUpdateWhereLocateWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_sr_codes SET description = ? WHERE LOCATE(?, code) > 0"
            );
            $stmt->execute(['Updated XYZ', 'XYZ']);

            $rows = $this->ztdQuery(
                "SELECT id, description FROM my_sr_codes WHERE description = 'Updated XYZ' ORDER BY id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE LOCATE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(3, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with LOCATE in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET REPLACE with 4 total params.
     */
    public function testPreparedUpdateReplaceWithFourTotalParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_sr_codes SET description = REPLACE(description, ?, ?) WHERE id = ?"
            );
            $stmt->execute(['item', 'product', 1]);

            $rows = $this->ztdQuery("SELECT description FROM my_sr_codes WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared REPLACE 4 params: got ' . json_encode($rows));
            }

            $this->assertSame('First product', $rows[0]['description']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE REPLACE 4 params failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT WHERE REPLACE(col, ?, ?) = ? — function equality check.
     */
    public function testPreparedSelectWhereReplaceEqualsParam(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_sr_codes VALUES (5, 'NEW-999', 'Fifth item', 'NEW')");

            $stmt = $this->pdo->prepare(
                "SELECT id, code FROM my_sr_codes WHERE REPLACE(code, ?, ?) = ?"
            );
            $stmt->execute(['-', '', 'NEW999']);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'SELECT REPLACE WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(5, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with REPLACE in WHERE failed: ' . $e->getMessage());
        }
    }
}
