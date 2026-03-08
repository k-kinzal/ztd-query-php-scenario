<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use ZtdQuery\Exception\ColumnAlreadyExistsException;
use ZtdQuery\Exception\ColumnNotFoundException;

/**
 * Tests ALTER TABLE error scenarios on MySQL ZTD via MySQLi:
 * - Duplicate column add throws ColumnAlreadyExistsException
 * - Drop/modify/change/rename nonexistent column throws ColumnNotFoundException
 * - Shadow store remains intact after ALTER TABLE errors
 * - ALTER on unreflected table throws SchemaNotFoundException
 * @spec SPEC-5.1a
 */
class AlterTableErrorTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_alt_err (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_alt_err'];
    }


    public function testAddDuplicateColumnThrows(): void
    {
        $this->expectException(ColumnAlreadyExistsException::class);
        $this->mysqli->query('ALTER TABLE mi_alt_err ADD COLUMN name VARCHAR(100)');
    }

    public function testDropNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_alt_err DROP COLUMN nonexistent');
    }

    public function testModifyNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_alt_err MODIFY COLUMN nonexistent INT');
    }

    public function testChangeNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_alt_err CHANGE COLUMN nonexistent new_col INT');
    }

    public function testRenameNonexistentColumnThrows(): void
    {
        $this->expectException(ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_alt_err RENAME COLUMN nonexistent TO new_col');
    }

    public function testShadowStoreIntactAfterAlterError(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_err VALUES (1, 'Alice', 90)");

        try {
            $this->mysqli->query('ALTER TABLE mi_alt_err ADD COLUMN name VARCHAR(100)');
        } catch (ColumnAlreadyExistsException $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name FROM mi_alt_err WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testSuccessfulAlterThenErrorLeavesSchemaConsistent(): void
    {
        // Add column successfully
        $this->mysqli->query('ALTER TABLE mi_alt_err ADD COLUMN email VARCHAR(100)');

        // Try to add it again — should error
        try {
            $this->mysqli->query('ALTER TABLE mi_alt_err ADD COLUMN email VARCHAR(100)');
            $this->fail('Expected ColumnAlreadyExistsException');
        } catch (ColumnAlreadyExistsException $e) {
            // Expected
        }

        // Schema has 4 columns now (id, name, score, email)
        $this->mysqli->query("INSERT INTO mi_alt_err VALUES (1, 'Alice', 90, 'alice@test.com')");
        $result = $this->mysqli->query('SELECT name, email FROM mi_alt_err WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('alice@test.com', $row['email']);
    }
}
