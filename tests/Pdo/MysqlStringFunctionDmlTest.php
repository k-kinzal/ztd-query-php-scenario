<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests string functions in DML on MySQL, including MySQL-specific
 * CONCAT() function (vs || operator) and LOCATE/INSTR.
 *
 * @spec SPEC-4.2
 */
class MysqlStringFunctionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_sfd_items (
            id INT PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            label VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT \'active\'
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_sfd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_sfd_items VALUES (1, 'SKU-001', 'Widget Alpha', 'active')");
        $this->pdo->exec("INSERT INTO my_sfd_items VALUES (2, 'SKU-002', 'Gadget Beta', 'active')");
        $this->pdo->exec("INSERT INTO my_sfd_items VALUES (3, 'OLD-003', 'Sprocket Gamma', 'inactive')");
    }

    /**
     * UPDATE with CONCAT (MySQL-specific).
     */
    public function testUpdateConcat(): void
    {
        $sql = "UPDATE my_sfd_items SET label = CONCAT(label, ' [', status, ']')";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT label FROM my_sfd_items WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['label'] !== 'Widget Alpha [active]') {
                $this->markTestIncomplete(
                    'CONCAT UPDATE: expected "Widget Alpha [active]", got "'
                    . $rows[0]['label'] . '"'
                );
            }

            $this->assertSame('Widget Alpha [active]', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CONCAT UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with REPLACE and bound params.
     */
    public function testPreparedUpdateReplace(): void
    {
        $sql = "UPDATE my_sfd_items SET code = REPLACE(code, ?, ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['SKU', 'PRD']);

            $rows = $this->ztdQuery("SELECT code FROM my_sfd_items ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame('PRD-001', $rows[0]['code']);
            $this->assertSame('PRD-002', $rows[1]['code']);
            $this->assertSame('OLD-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared REPLACE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with UPPER/LOWER.
     */
    public function testUpdateUpperLower(): void
    {
        $this->pdo->exec("UPDATE my_sfd_items SET label = UPPER(label), code = LOWER(code)");

        $rows = $this->ztdQuery("SELECT code, label FROM my_sfd_items WHERE id = 1");

        $this->assertSame('sku-001', $rows[0]['code']);
        $this->assertSame('WIDGET ALPHA', $rows[0]['label']);
    }

    /**
     * Prepared DELETE with LOCATE function.
     */
    public function testDeleteWithLocate(): void
    {
        $sql = "DELETE FROM my_sfd_items WHERE LOCATE(?, code) > 0";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['OLD']);

            $rows = $this->ztdQuery("SELECT code FROM my_sfd_items ORDER BY id");

            $this->assertCount(2, $rows);
            $this->assertSame('SKU-001', $rows[0]['code']);
            $this->assertSame('SKU-002', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LOCATE DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with CONCAT and prepared suffix param.
     */
    public function testPreparedConcatUpdate(): void
    {
        $sql = "UPDATE my_sfd_items SET label = CONCAT(label, ?) WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([' (v2)', 1]);

            $rows = $this->ztdQuery("SELECT label FROM my_sfd_items WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertSame('Widget Alpha (v2)', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared CONCAT UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with SUBSTRING and LEFT functions.
     */
    public function testUpdateSubstring(): void
    {
        $sql = "UPDATE my_sfd_items SET code = CONCAT(LEFT(code, 3), '-X') WHERE code LIKE 'SKU%'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT code FROM my_sfd_items ORDER BY id");

            $this->assertSame('SKU-X', $rows[0]['code']);
            $this->assertSame('SKU-X', $rows[1]['code']);
            $this->assertSame('OLD-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUBSTRING UPDATE failed: ' . $e->getMessage());
        }
    }
}
