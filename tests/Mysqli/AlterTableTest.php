<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/** @spec SPEC-5.1a */
class AlterTableTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE alter_test (id INT PRIMARY KEY, name VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['alter_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO alter_test (id, name) VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO alter_test (id, name) VALUES (2, 'Bob')");
    }

    public function testAddColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN age INT');

        // Insert with the new column
        $this->mysqli->query("INSERT INTO alter_test (id, name, age) VALUES (3, 'Charlie', 30)");

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(30, (int) $row['age']);
    }

    public function testDropColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test DROP COLUMN name');

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertArrayHasKey('id', $row);
        $this->assertArrayNotHasKey('name', $row);
    }

    public function testModifyColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test MODIFY COLUMN name TEXT');

        // Should still be able to query
        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testChangeColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test CHANGE COLUMN name full_name VARCHAR(500)');

        // Insert with new column name
        $this->mysqli->query("INSERT INTO alter_test (id, full_name) VALUES (3, 'Charlie Brown')");

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie Brown', $row['full_name']);
    }

    public function testAddAndDropColumnSequence(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN email VARCHAR(255)');
        $this->mysqli->query("INSERT INTO alter_test (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')");

        $result = $this->mysqli->query('SELECT email FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('charlie@example.com', $row['email']);

        // Drop the column we just added
        $this->mysqli->query('ALTER TABLE alter_test DROP COLUMN email');

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertArrayNotHasKey('email', $row);
    }

    public function testAlterTableIsolation(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN age INT');

        // Physical table should be unchanged
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SHOW COLUMNS FROM alter_test');
        $columns = [];
        while ($row = $result->fetch_assoc()) {
            $columns[] = $row['Field'];
        }
        $this->assertNotContains('age', $columns);
        $this->mysqli->enableZtd();
    }
}
