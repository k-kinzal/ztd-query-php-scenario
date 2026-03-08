<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT INTO ... SELECT FROM the same table on MySQLi.
 *
 * Self-referencing INSERT copies rows from a table back into itself.
 * @spec SPEC-4.1
 */
class SelfReferencingInsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_sri_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['mi_sri_test'];
    }


    /**
     * Self-referencing INSERT with new IDs.
     */
    public function testSelfReferencingInsertWithNewIds(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");

        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM mi_sri_test'
        );
        $this->assertSame(2, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
        $this->assertSame(4, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Self-referencing INSERT with WHERE filter.
     */
    public function testSelfReferencingInsertWithFilter(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (3, 'Charlie', 70, 'A')");

        $this->mysqli->query(
            "INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, 'A-copy' FROM mi_sri_test WHERE category = 'A'"
        );
        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    /**
     * Self-referencing INSERT doesn't loop infinitely.
     */
    public function testSelfReferencingInsertDoesNotLoop(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");

        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 10, name, score, category FROM mi_sri_test'
        );
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->mysqli->query(
            'INSERT INTO mi_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM mi_sri_test'
        );

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sri_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
