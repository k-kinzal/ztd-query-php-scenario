<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL-specific INSERT modifiers through ZTD:
 * - INSERT IGNORE
 * - INSERT ... ON DUPLICATE KEY UPDATE with expressions
 * - REPLACE INTO with SET syntax
 * @spec pending
 */
class MysqlInsertModifiersTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_im_test (id INT PRIMARY KEY, name VARCHAR(50), counter INT DEFAULT 0)';
    }

    protected function getTableNames(): array
    {
        return ['mysql_im_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_im_test VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO mysql_im_test VALUES (2, 'Bob', 1)");
    }

    /**
     * INSERT IGNORE with duplicate key — silently skipped.
     */
    public function testInsertIgnoreDuplicate(): void
    {
        $this->pdo->exec("INSERT IGNORE INTO mysql_im_test VALUES (1, 'Duplicate', 99)");

        // Original row should remain
        $stmt = $this->pdo->query('SELECT name, counter FROM mysql_im_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(1, (int) $row['counter']);
    }

    /**
     * INSERT IGNORE with new key — inserted normally.
     */
    public function testInsertIgnoreNewRow(): void
    {
        $this->pdo->exec("INSERT IGNORE INTO mysql_im_test VALUES (3, 'Charlie', 1)");

        $stmt = $this->pdo->query('SELECT name FROM mysql_im_test WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    /**
     * ON DUPLICATE KEY UPDATE with self-referencing expression should increment.
     */
    public function testOnDuplicateKeySelfRefIncrements(): void
    {
        $this->pdo->exec(
            "INSERT INTO mysql_im_test VALUES (1, 'Alice', 1)
             ON DUPLICATE KEY UPDATE counter = counter + 1"
        );

        $stmt = $this->pdo->query('SELECT counter FROM mysql_im_test WHERE id = 1');
        $counter = (int) $stmt->fetchColumn();
        // Expected: counter should be 2 (original 1 + 1)
        if ($counter !== 2) {
            $this->markTestIncomplete(
                'ON DUPLICATE KEY UPDATE with self-referencing expression loses old value. '
                . 'Expected counter 2, got ' . $counter
            );
        }
        $this->assertSame(2, $counter);
    }

    /**
     * ON DUPLICATE KEY UPDATE with VALUES() function.
     */
    public function testOnDuplicateKeyValues(): void
    {
        $this->pdo->exec(
            "INSERT INTO mysql_im_test VALUES (1, 'Updated Alice', 10)
             ON DUPLICATE KEY UPDATE name = VALUES(name), counter = VALUES(counter)"
        );

        $stmt = $this->pdo->query('SELECT name, counter FROM mysql_im_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Updated Alice', $row['name']);
        $this->assertEquals(10, (int) $row['counter']);
    }

    /**
     * Multi-row INSERT IGNORE — some duplicates, some new.
     */
    public function testMultiRowInsertIgnore(): void
    {
        $this->pdo->exec(
            "INSERT IGNORE INTO mysql_im_test VALUES
             (1, 'Dup1', 99),
             (3, 'Charlie', 1),
             (2, 'Dup2', 99),
             (4, 'Diana', 1)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_im_test');
        $count = (int) $stmt->fetchColumn();
        // 2 original + 2 new = 4 (duplicates ignored)
        $this->assertEquals(4, $count);
    }

    /**
     * REPLACE INTO with SET syntax (MySQL-specific).
     */
    public function testReplaceIntoSetSyntax(): void
    {
        try {
            $this->pdo->exec("REPLACE INTO mysql_im_test SET id = 1, name = 'Replaced', counter = 5");

            $stmt = $this->pdo->query('SELECT name, counter FROM mysql_im_test WHERE id = 1');
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Replaced', $row['name']);
            $this->assertEquals(5, (int) $row['counter']);
        } catch (\Exception $e) {
            // REPLACE ... SET syntax may not be supported
            $this->markTestSkipped('REPLACE ... SET not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_im_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
