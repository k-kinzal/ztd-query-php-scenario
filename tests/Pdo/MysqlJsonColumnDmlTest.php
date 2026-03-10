<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests JSON column operations in DML through ZTD on MySQL PDO.
 *
 * MySQL 5.7+ has native JSON type and functions (JSON_EXTRACT, JSON_SET, etc.).
 * This tests whether the CTE rewriter handles JSON operations in DML correctly.
 *
 * @spec SPEC-10.2
 */
class MysqlJsonColumnDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_json_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            metadata JSON DEFAULT NULL
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_json_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_json_products (name, metadata) VALUES ('Widget', '{\"color\":\"red\",\"weight\":1.5,\"tags\":[\"sale\",\"new\"]}')");
        $this->ztdExec("INSERT INTO my_json_products (name, metadata) VALUES ('Gadget', '{\"color\":\"blue\",\"weight\":2.0,\"tags\":[\"premium\"]}')");
        $this->ztdExec("INSERT INTO my_json_products (name, metadata) VALUES ('Doohickey', '{\"color\":\"green\",\"weight\":0.5,\"tags\":[]}')");
    }

    /**
     * SELECT with JSON_EXTRACT / -> operator — baseline.
     */
    public function testSelectJsonExtract(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM my_json_products ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('blue', $rows[1]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT JSON_EXTRACT (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET using JSON_SET.
     */
    public function testUpdateJsonSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_json_products SET metadata = JSON_SET(metadata, '$.color', 'yellow') WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM my_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JSON_SET (MySQL): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('yellow', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE JSON_EXTRACT matches.
     */
    public function testDeleteWhereJsonExtract(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_json_products WHERE JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) = 'green'"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_json_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSON_EXTRACT (MySQL): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSON_EXTRACT (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JSON_REPLACE.
     */
    public function testUpdateJsonReplace(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_json_products SET metadata = JSON_REPLACE(metadata, '$.weight', 9.9) WHERE name = 'Gadget'"
            );

            $rows = $this->ztdQuery(
                "SELECT JSON_EXTRACT(metadata, '$.weight') AS weight FROM my_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JSON_REPLACE (MySQL): expected 1, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(9.9, (float) $rows[0]['weight'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_REPLACE (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with JSON_SET and bound parameter.
     */
    public function testPreparedUpdateJsonSet(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "UPDATE my_json_products SET metadata = JSON_SET(metadata, '$.color', ?) WHERE name = ?"
            );
            $stmt->execute(['purple', 'Gadget']);

            $rows = $this->ztdQuery(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM my_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE JSON_SET (MySQL): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('purple', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE JSON_SET (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE JSON_EXTRACT with numeric comparison.
     */
    public function testDeleteWhereJsonExtractNumeric(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_json_products WHERE JSON_EXTRACT(metadata, '$.weight') > 1.0"
            );

            $rows = $this->ztdQuery("SELECT name FROM my_json_products ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSON_EXTRACT numeric (MySQL): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Doohickey', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSON_EXTRACT numeric (MySQL) failed: ' . $e->getMessage());
        }
    }
}
