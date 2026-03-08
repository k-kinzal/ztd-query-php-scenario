<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT with DEFAULT keyword on PostgreSQL PDO ZTD.
 *
 * Limitation: InsertTransformer converts VALUES(DEFAULT) to SELECT DEFAULT AS "col"
 * which PostgreSQL rejects: "DEFAULT is not allowed in this context".
 *
 * Both INSERT ... DEFAULT VALUES and INSERT ... VALUES (DEFAULT) fail under ZTD.
 * @spec pending
 */
class PostgresInsertDefaultValuesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_idef_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) DEFAULT \\\'default_name\\\',
            score INT DEFAULT 100
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_idef_test'];
    }


    /**
     * INSERT ... DEFAULT VALUES fails under ZTD.
     *
     * InsertTransformer requires explicit values to project into the CTE.
     */
    public function testInsertDefaultValuesFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('INSERT INTO pg_idef_test DEFAULT VALUES');
    }

    /**
     * INSERT with DEFAULT keyword in VALUES fails under ZTD.
     *
     * InsertTransformer converts to SELECT DEFAULT AS "col" which
     * PostgreSQL rejects: "DEFAULT is not allowed in this context".
     */
    public function testInsertWithDefaultInValuesFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("INSERT INTO pg_idef_test (name, score) VALUES (DEFAULT, 50)");
    }

    /**
     * INSERT with all DEFAULT values fails.
     */
    public function testInsertWithAllDefaultsFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('INSERT INTO pg_idef_test (name, score) VALUES (DEFAULT, DEFAULT)');
    }

    /**
     * INSERT with mix of explicit and DEFAULT fails.
     */
    public function testInsertWithMixedDefaultFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("INSERT INTO pg_idef_test (name, score) VALUES ('Alice', DEFAULT)");
    }

    /**
     * INSERT with only explicit values works normally.
     */
    public function testInsertWithExplicitValuesWorks(): void
    {
        $this->pdo->exec("INSERT INTO pg_idef_test (name, score) VALUES ('Alice', 90)");

        $stmt = $this->pdo->query("SELECT name, score FROM pg_idef_test WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Physical isolation with explicit values.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_idef_test (name, score) VALUES ('Bob', 80)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_idef_test');
        $this->assertGreaterThanOrEqual(1, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_idef_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
