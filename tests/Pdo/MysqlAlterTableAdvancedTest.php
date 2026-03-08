<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests advanced ALTER TABLE operations on MySQL ZTD:
 * - RENAME TABLE (ALTER TABLE ... RENAME TO ...)
 * - CHANGE COLUMN with existing shadow data (column rename + type change)
 * - MODIFY COLUMN with existing shadow data (type change only)
 * - ALTER TABLE after shadow INSERT (shadow data follows schema changes)
 * @spec SPEC-5.1a
 */
class MysqlAlterTableAdvancedTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE alt_adv_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['alt_adv_m', 'alt_adv_renamed'];
    }


    public function testRenameTable(): void
    {
        // Insert data into shadow
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        // Rename table
        $this->pdo->exec('ALTER TABLE alt_adv_m RENAME TO alt_adv_renamed');

        // Query by new name should return data
        $stmt = $this->pdo->query('SELECT name FROM alt_adv_renamed WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testChangeColumnWithData(): void
    {
        // Insert data into shadow
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        // CHANGE COLUMN renames and can change type
        $this->pdo->exec('ALTER TABLE alt_adv_m CHANGE COLUMN name full_name VARCHAR(100)');

        // Insert new row using new column name
        $this->pdo->exec("INSERT INTO alt_adv_m (id, full_name, score) VALUES (2, 'Bob', 80)");

        // Old data should have column renamed
        $stmt = $this->pdo->query('SELECT full_name FROM alt_adv_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['full_name']);

        // New row also accessible
        $stmt = $this->pdo->query('SELECT full_name FROM alt_adv_m WHERE id = 2');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['full_name']);
    }

    public function testModifyColumnWithData(): void
    {
        // Insert data
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        // MODIFY COLUMN changes type but keeps name
        $this->pdo->exec('ALTER TABLE alt_adv_m MODIFY COLUMN name TEXT');

        // Data should still be accessible
        $stmt = $this->pdo->query('SELECT name FROM alt_adv_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testDropColumnRemovesDataFromShadow(): void
    {
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        $this->pdo->exec('ALTER TABLE alt_adv_m DROP COLUMN score');

        $stmt = $this->pdo->query('SELECT * FROM alt_adv_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertArrayNotHasKey('score', $row);
        $this->assertSame('Alice', $row['name']);
    }

    public function testAddColumnThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        $this->pdo->exec('ALTER TABLE alt_adv_m ADD COLUMN email VARCHAR(100)');

        $this->pdo->exec("INSERT INTO alt_adv_m (id, name, score, email) VALUES (2, 'Bob', 80, 'bob@test.com')");

        $stmt = $this->pdo->query('SELECT email FROM alt_adv_m WHERE id = 2');
        $this->assertSame('bob@test.com', $stmt->fetchColumn());
    }

    public function testMultipleAlterOperations(): void
    {
        $this->pdo->exec("INSERT INTO alt_adv_m VALUES (1, 'Alice', 90)");

        // Add column, then rename existing column
        $this->pdo->exec('ALTER TABLE alt_adv_m ADD COLUMN email VARCHAR(100)');
        $this->pdo->exec('ALTER TABLE alt_adv_m RENAME COLUMN name TO full_name');

        $this->pdo->exec("INSERT INTO alt_adv_m (id, full_name, score, email) VALUES (2, 'Bob', 80, 'bob@test.com')");

        $stmt = $this->pdo->query('SELECT full_name, email FROM alt_adv_m WHERE id = 2');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['full_name']);
        $this->assertSame('bob@test.com', $row['email']);

        // Count all rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM alt_adv_m');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testAlterTablePhysicalIsolation(): void
    {
        $this->pdo->exec('ALTER TABLE alt_adv_m ADD COLUMN extra VARCHAR(50)');

        // Physical table should NOT have the new column
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM alt_adv_m LIMIT 0');
        $meta = [];
        for ($i = 0; $i < $stmt->columnCount(); $i++) {
            $meta[] = $stmt->getColumnMeta($i)['name'];
        }
        $this->assertNotContains('extra', $meta);
    }
}
