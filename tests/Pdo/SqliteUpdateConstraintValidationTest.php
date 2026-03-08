<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE constraint validation in the shadow store on SQLite.
 *
 * UpdateMutation optionally validates NOT NULL and UNIQUE constraints
 * when constraint validation is enabled. This tests whether ZTD
 * correctly enforces these constraints during UPDATE operations.
 * @spec SPEC-4.2
 */
class SqliteUpdateConstraintValidationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE upd_const_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['upd_const_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO upd_const_test (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO upd_const_test (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
        $this->pdo->exec("INSERT INTO upd_const_test (id, name, email, score) VALUES (3, 'Charlie', 'charlie@test.com', 70)");
    }
    /**
     * UPDATE with valid values succeeds.
     */
    public function testUpdateWithValidValues(): void
    {
        $this->pdo->exec("UPDATE upd_const_test SET name = 'Alicia', score = 95 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM upd_const_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alicia', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * UPDATE cross-column copy: SET col1 = col2.
     */
    public function testUpdateCrossColumnCopy(): void
    {
        $this->pdo->exec('UPDATE upd_const_test SET score = id * 10 WHERE id <= 2');

        $stmt = $this->pdo->query('SELECT id, score FROM upd_const_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(10, (int) $rows[0]['score']); // id=1 * 10
        $this->assertSame(20, (int) $rows[1]['score']); // id=2 * 10
        $this->assertSame(70, (int) $rows[2]['score']); // unchanged
    }

    /**
     * UPDATE with NULL on nullable column succeeds.
     */
    public function testUpdateNullableColumnToNull(): void
    {
        $this->pdo->exec('UPDATE upd_const_test SET score = NULL WHERE id = 1');

        $stmt = $this->pdo->query('SELECT score FROM upd_const_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['score']);
    }

    /**
     * UPDATE email to unique value succeeds.
     */
    public function testUpdateUniqueColumnToUniqueValue(): void
    {
        $this->pdo->exec("UPDATE upd_const_test SET email = 'alicia@test.com' WHERE id = 1");

        $stmt = $this->pdo->query("SELECT email FROM upd_const_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('alicia@test.com', $row['email']);
    }

    /**
     * UPDATE email to NULL on UNIQUE column succeeds (NULL is not a duplicate).
     */
    public function testUpdateUniqueColumnToNull(): void
    {
        $this->pdo->exec('UPDATE upd_const_test SET email = NULL WHERE id = 1');

        $stmt = $this->pdo->query('SELECT email FROM upd_const_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['email']);
    }

    /**
     * Multiple sequential UPDATEs on the same row.
     */
    public function testSequentialUpdatesOnSameRow(): void
    {
        $this->pdo->exec("UPDATE upd_const_test SET name = 'Step1' WHERE id = 1");
        $this->pdo->exec('UPDATE upd_const_test SET score = 100 WHERE id = 1');
        $this->pdo->exec("UPDATE upd_const_test SET name = 'Step3', score = 200 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM upd_const_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Step3', $row['name']);
        $this->assertSame(200, (int) $row['score']);
    }

    /**
     * UPDATE all rows at once.
     */
    public function testUpdateAllRows(): void
    {
        $affected = $this->pdo->exec('UPDATE upd_const_test SET score = score + 10');

        $this->assertSame(3, $affected);

        $stmt = $this->pdo->query('SELECT score FROM upd_const_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(100, (int) $rows[0]['score']);
        $this->assertSame(90, (int) $rows[1]['score']);
        $this->assertSame(80, (int) $rows[2]['score']);
    }

    /**
     * Physical isolation after UPDATE.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE upd_const_test SET name = 'Modified' WHERE id = 1");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM upd_const_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
