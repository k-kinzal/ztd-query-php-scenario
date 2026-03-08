<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Scenarios\BasicCrudScenario;
use Tests\Support\AbstractMysqliTestCase;

/** @spec SPEC-1.1, SPEC-1.2, SPEC-2.1, SPEC-2.2, SPEC-2.3, SPEC-3.1, SPEC-4.1, SPEC-4.2, SPEC-4.3 */
class BasicCrudTest extends AbstractMysqliTestCase
{
    use BasicCrudScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['users'];
    }

    public function testAffectedRowCount(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $this->mysqli->query("UPDATE users SET name = 'Updated' WHERE id > 0");

        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testPreparedStatementWithBindParam(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->mysqli->prepare('SELECT * FROM users WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();

        $this->assertSame(1, (int) $row['id']);
        $this->assertSame('Alice', $row['name']);
    }
}
