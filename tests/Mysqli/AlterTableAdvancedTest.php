<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests advanced ALTER TABLE operations on MySQL ZTD via MySQLi:
 * - RENAME TABLE (ALTER TABLE ... RENAME TO ...)
 * - CHANGE COLUMN with existing shadow data
 * - MODIFY COLUMN with existing shadow data
 * - Multiple ALTER operations in sequence
 * @spec SPEC-5.1a
 */
class AlterTableAdvancedTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_alt_adv (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_alt_adv', 'mi_alt_adv_new'];
    }


    public function testRenameTable(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv RENAME TO mi_alt_adv_new');

        $result = $this->mysqli->query('SELECT name FROM mi_alt_adv_new WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testChangeColumnWithData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv CHANGE COLUMN name full_name VARCHAR(100)');

        $this->mysqli->query("INSERT INTO mi_alt_adv (id, full_name, score) VALUES (2, 'Bob', 80)");

        $result = $this->mysqli->query('SELECT full_name FROM mi_alt_adv WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['full_name']);

        $result = $this->mysqli->query('SELECT full_name FROM mi_alt_adv WHERE id = 2');
        $this->assertSame('Bob', $result->fetch_assoc()['full_name']);
    }

    public function testModifyColumnWithData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv MODIFY COLUMN name TEXT');

        $result = $this->mysqli->query('SELECT name FROM mi_alt_adv WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testDropColumnRemovesShadowData(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv DROP COLUMN score');

        $result = $this->mysqli->query('SELECT * FROM mi_alt_adv WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertArrayNotHasKey('score', $row);
        $this->assertSame('Alice', $row['name']);
    }

    public function testMultipleAlterOperations(): void
    {
        $this->mysqli->query("INSERT INTO mi_alt_adv VALUES (1, 'Alice', 90)");

        $this->mysqli->query('ALTER TABLE mi_alt_adv ADD COLUMN email VARCHAR(100)');
        $this->mysqli->query('ALTER TABLE mi_alt_adv RENAME COLUMN name TO full_name');

        $this->mysqli->query("INSERT INTO mi_alt_adv (id, full_name, score, email) VALUES (2, 'Bob', 80, 'bob@test.com')");

        $result = $this->mysqli->query('SELECT full_name, email FROM mi_alt_adv WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['full_name']);
        $this->assertSame('bob@test.com', $row['email']);

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_alt_adv');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }
}
