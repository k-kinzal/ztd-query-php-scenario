<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DDL operations mid-session: DROP TABLE + CREATE TABLE, TRUNCATE,
 * and how they interact with existing shadow data and prepared statements.
 * @spec pending
 */
class SqliteDdlMidSessionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ddl_sess (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE ddl_sess (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE ddl_sess (id INT PRIMARY KEY, email VARCHAR(100), active INT)',
            'CREATE TABLE ddl_cycle (id INT PRIMARY KEY, val INT)',
            'CREATE TABLE ddl_other (id INT PRIMARY KEY, ref_id INT, tag VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ddl_sess', 'ddl_cycle', 'ddl_other'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE ddl_sess (id INT PRIMARY KEY, name VARCHAR(50), score INT)');

        }

    public function testDropTableClearsShadowAndQueryFails(): void
    {
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (2, 'Bob', 85)");

        $this->pdo->exec('DROP TABLE ddl_sess');

        // After DROP TABLE on a shadow-created table, the table no longer exists
        // (neither in shadow nor physical DB). Querying throws "no such table".
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such table');
        $this->pdo->query('SELECT COUNT(*) FROM ddl_sess');
    }

    public function testDropThenRecreateTable(): void
    {
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_sess');
        $this->pdo->exec('CREATE TABLE ddl_sess (id INT PRIMARY KEY, name VARCHAR(50))');

        // Fresh table with new schema
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'NewAlice')");

        $stmt = $this->pdo->query('SELECT name FROM ddl_sess WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('NewAlice', $row['name']);
    }

    public function testCreateTableWithDifferentSchema(): void
    {
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'Alice', 100)");

        $this->pdo->exec('DROP TABLE ddl_sess');
        $this->pdo->exec('CREATE TABLE ddl_sess (id INT PRIMARY KEY, email VARCHAR(100), active INT)');

        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'alice@test.com', 1)");

        $stmt = $this->pdo->query('SELECT email, active FROM ddl_sess WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('alice@test.com', $row['email']);
        $this->assertSame(1, (int) $row['active']);
    }

    public function testMultipleDropCreateCycles(): void
    {
        for ($cycle = 0; $cycle < 3; $cycle++) {
            if ($cycle > 0) {
                $this->pdo->exec('DROP TABLE ddl_cycle');
            }
            $this->pdo->exec('CREATE TABLE ddl_cycle (id INT PRIMARY KEY, val INT)');
            $this->pdo->exec("INSERT INTO ddl_cycle VALUES (1, $cycle)");

            $stmt = $this->pdo->query('SELECT val FROM ddl_cycle WHERE id = 1');
            $val = (int) $stmt->fetchColumn();
            $this->assertSame($cycle, $val);
        }
    }

    public function testInsertAfterDropWithoutRecreateThrows(): void
    {
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'Alice', 100)");
        $this->pdo->exec('DROP TABLE ddl_sess');

        // After DROP on a shadow-created table, INSERT throws because
        // ZTD no longer has schema info for the table
        $this->expectException(\ZtdQuery\Adapter\Pdo\ZtdPdoException::class);
        $this->expectExceptionMessage('Cannot determine columns');
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (2, 'Bob', 85)");
    }

    public function testShadowDataPersistenceAcrossNewTable(): void
    {
        // Create two tables, insert into both
        $this->pdo->exec('CREATE TABLE ddl_other (id INT PRIMARY KEY, ref_id INT, tag VARCHAR(20))');
        $this->pdo->exec("INSERT INTO ddl_sess VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO ddl_other VALUES (1, 1, 'important')");

        // Drop first table
        $this->pdo->exec('DROP TABLE ddl_sess');

        // Second table's shadow data should be unaffected
        $stmt = $this->pdo->query('SELECT tag FROM ddl_other WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('important', $row['tag']);
    }
}
