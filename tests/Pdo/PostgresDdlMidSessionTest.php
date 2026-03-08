<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DDL operations mid-session on PostgreSQL ZTD PDO.
 * @spec pending
 */
class PostgresDdlMidSessionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ddl_ms_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE succeeds (creates a new shadow table even though physical table exists)
        $this->pdo->exec(',
            'CREATE TABLE ddl_ms_pg (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE ddl_other_pg (id INT PRIMARY KEY, tag VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ddl_ms_pg', 'succeeds', 'ddl_other_pg'];
    }


    public function testDropTableClearsShadowAndFallsToPhysical(): void
    {
        $this->pdo->exec("INSERT INTO ddl_ms_pg VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_ms_pg');

        // After DROP the query falls through to the physical table
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ddl_ms_pg');
        $count = (int) $stmt->fetchColumn();
        // Physical table has 0 rows (shadow INSERT didn't reach it)
        $this->assertSame(0, $count);
    }

    public function testDropAndRecreateTableInShadow(): void
    {
        $this->pdo->exec("INSERT INTO ddl_ms_pg VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_ms_pg');

        // DROP clears the shadow knowledge of the table,
        // and CREATE TABLE succeeds (creates a new shadow table even though physical table exists)
        $this->pdo->exec('CREATE TABLE ddl_ms_pg (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ddl_ms_pg VALUES (1, 'NewAlice')");

        $stmt = $this->pdo->query('SELECT name FROM ddl_ms_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('NewAlice', $row['name']);
    }

    public function testShadowDataPersistenceAcrossTableDrop(): void
    {
        // Create a shadow-only table
        $this->pdo->exec('CREATE TABLE ddl_other_pg (id INT PRIMARY KEY, tag VARCHAR(20))');
        $this->pdo->exec("INSERT INTO ddl_other_pg VALUES (1, 'important')");

        // Insert into physical-reflected table
        $this->pdo->exec("INSERT INTO ddl_ms_pg VALUES (1, 'Alice', 100)");

        // Drop the physical table from shadow
        $this->pdo->exec('DROP TABLE ddl_ms_pg');

        // Shadow-only table data should be unaffected
        $stmt = $this->pdo->query('SELECT tag FROM ddl_other_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('important', $row['tag']);
    }
}
