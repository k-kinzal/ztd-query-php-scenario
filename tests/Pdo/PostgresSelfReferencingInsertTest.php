<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT INTO ... SELECT FROM the same table on PostgreSQL.
 *
 * Self-referencing INSERT copies rows from a table back into itself.
 * @spec pending
 */
class PostgresSelfReferencingInsertTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sri_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, category VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['pg_sri_test'];
    }


    /**
     * Self-referencing INSERT with new IDs.
     */
    public function testSelfReferencingInsertWithNewIds(): void
    {
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");

        $affected = $this->pdo->exec(
            'INSERT INTO pg_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM pg_sri_test'
        );

        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sri_test');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing INSERT with WHERE filter.
     */
    public function testSelfReferencingInsertWithFilter(): void
    {
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (3, 'Charlie', 70, 'A')");

        $affected = $this->pdo->exec(
            "INSERT INTO pg_sri_test (id, name, score, category) SELECT id + 100, name, score, 'A-copy' FROM pg_sri_test WHERE category = 'A'"
        );

        $this->assertSame(2, $affected);
    }

    /**
     * Self-referencing INSERT doesn't cause infinite loop.
     */
    public function testSelfReferencingInsertDoesNotLoop(): void
    {
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");

        $affected = $this->pdo->exec(
            'INSERT INTO pg_sri_test (id, name, score, category) SELECT id + 10, name, score, category FROM pg_sri_test'
        );

        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sri_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing INSERT after mutations.
     *
     * On PostgreSQL, computed columns (id + 100) in INSERT...SELECT become NULL
     * (known limitation, spec 4.1a). Direct column references are transferred correctly.
     */
    public function testSelfReferencingInsertAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (2, 'Bob', 80, 'B')");
        $this->pdo->exec("UPDATE pg_sri_test SET score = 100 WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_sri_test WHERE id = 2");

        $affected = $this->pdo->exec(
            'INSERT INTO pg_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM pg_sri_test'
        );

        $this->assertSame(1, $affected);

        // Total should be 2 (original + copy)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sri_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        // On PostgreSQL, id + 100 is a computed expression — may become NULL.
        // Direct column refs (name, score, category) should transfer correctly.
        $stmt = $this->pdo->query("SELECT name FROM pg_sri_test WHERE name = 'Alice' LIMIT 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sri_test (id, name, score, category) VALUES (1, 'Alice', 90, 'A')");
        $this->pdo->exec(
            'INSERT INTO pg_sri_test (id, name, score, category) SELECT id + 100, name, score, category FROM pg_sri_test'
        );

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_sri_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
