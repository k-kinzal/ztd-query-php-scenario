<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests DDL-then-DML workflows in a single ZTD session on SQLite.
 *
 * Users commonly:
 *   1. CREATE TABLE
 *   2. INSERT data
 *   3. SELECT data
 *   4. ALTER TABLE (add columns)
 *   5. INSERT/UPDATE with new columns
 *   6. DROP TABLE
 *
 * All of these should work within a single ZTD session.
 */
class SqliteDdlThenDmlWorkflowTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * CREATE TABLE → INSERT → SELECT workflow.
     */
    public function testCreateInsertSelect(): void
    {
        $this->pdo->exec('CREATE TABLE wf_test (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo->exec("INSERT INTO wf_test (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO wf_test (id, name) VALUES (2, 'Bob')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM wf_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM wf_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE → INSERT → UPDATE → SELECT workflow.
     */
    public function testCreateInsertUpdateSelect(): void
    {
        $this->pdo->exec('CREATE TABLE wf_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
        $this->pdo->exec("INSERT INTO wf_test (id, name, score) VALUES (1, 'Alice', 50)");

        $this->pdo->exec("UPDATE wf_test SET score = 90 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT score FROM wf_test WHERE id = 1');
        $this->assertSame(90, (int) $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE → INSERT → DELETE → SELECT workflow.
     */
    public function testCreateInsertDeleteSelect(): void
    {
        $this->pdo->exec('CREATE TABLE wf_test (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo->exec("INSERT INTO wf_test (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO wf_test (id, name) VALUES (2, 'Bob')");

        $this->pdo->exec('DELETE FROM wf_test WHERE id = 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM wf_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM wf_test WHERE id = 2');
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * CREATE two tables → INSERT into both → JOIN them.
     */
    public function testCreateTwoTablesJoinThem(): void
    {
        $this->pdo->exec('CREATE TABLE wf_users (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo->exec('CREATE TABLE wf_scores (user_id INTEGER, score INTEGER)');

        $this->pdo->exec("INSERT INTO wf_users (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO wf_users (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec('INSERT INTO wf_scores (user_id, score) VALUES (1, 90)');
        $this->pdo->exec('INSERT INTO wf_scores (user_id, score) VALUES (2, 80)');

        $stmt = $this->pdo->query('SELECT u.name, s.score FROM wf_users u JOIN wf_scores s ON u.id = s.user_id ORDER BY u.id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(90, (int) $rows[0]['score']);
    }

    /**
     * CREATE TABLE → INSERT → DROP TABLE → verify gone.
     */
    public function testCreateInsertDropTable(): void
    {
        $this->pdo->exec('CREATE TABLE wf_temp (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO wf_temp (id, val) VALUES (1, 'test')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM wf_temp');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $this->pdo->exec('DROP TABLE wf_temp');

        // After DROP, querying should fail
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT * FROM wf_temp');
    }

    /**
     * CREATE TABLE → INSERT → DROP → CREATE same name → INSERT new data.
     */
    public function testDropAndRecreateTable(): void
    {
        $this->pdo->exec('CREATE TABLE wf_recycle (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO wf_recycle (id, val) VALUES (1, 'first')");

        $this->pdo->exec('DROP TABLE wf_recycle');
        $this->pdo->exec('CREATE TABLE wf_recycle (id INTEGER PRIMARY KEY, val TEXT, extra TEXT)');
        $this->pdo->exec("INSERT INTO wf_recycle (id, val, extra) VALUES (1, 'second', 'bonus')");

        $stmt = $this->pdo->query('SELECT val, extra FROM wf_recycle WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('second', $row['val']);
        $this->assertSame('bonus', $row['extra']);
    }

    /**
     * Physical isolation — DDL + DML all in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('CREATE TABLE wf_iso (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO wf_iso (id, val) VALUES (1, 'shadow')");

        $this->pdo->disableZtd();

        // Table doesn't exist physically (created in shadow)
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT * FROM wf_iso');
    }
}
