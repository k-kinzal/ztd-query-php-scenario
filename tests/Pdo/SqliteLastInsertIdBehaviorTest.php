<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests lastInsertId() behavior with ZTD on SQLite.
 *
 * ZtdPdo::lastInsertId() delegates to the inner PDO connection.
 * Since ZTD rewrites INSERT into CTE-based SELECT queries, no physical
 * INSERT occurs. Therefore, the database's auto-increment counter is
 * NOT updated by shadow INSERT operations.
 *
 * This is a significant user pitfall: after a ZTD INSERT with an
 * AUTO_INCREMENT / AUTOINCREMENT column, lastInsertId() will NOT
 * return the expected auto-increment value.
 * @spec SPEC-4.7
 */
class SqliteLastInsertIdBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE lid_test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['lid_test'];
    }


    /**
     * lastInsertId() returns "0" after shadow INSERT because no physical INSERT occurs.
     */
    public function testLastInsertIdReturnsZeroAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Alice', 90)");

        // lastInsertId delegates to the inner PDO — shadow INSERT doesn't update it
        $id = $this->pdo->lastInsertId();
        $this->assertSame('0', $id);
    }

    /**
     * lastInsertId() does not increment across multiple shadow INSERTs.
     */
    public function testLastInsertIdDoesNotIncrementAcrossInserts(): void
    {
        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Alice', 90)");
        $id1 = $this->pdo->lastInsertId();

        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Bob', 80)");
        $id2 = $this->pdo->lastInsertId();

        // Both should be "0" since no physical INSERT occurred
        $this->assertSame('0', $id1);
        $this->assertSame('0', $id2);
    }

    /**
     * lastInsertId() returns correct value after physical INSERT (ZTD disabled).
     */
    public function testLastInsertIdWorksWithZtdDisabled(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Alice', 90)");

        $id = $this->pdo->lastInsertId();
        $this->assertSame('1', $id);

        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Bob', 80)");
        $id = $this->pdo->lastInsertId();
        $this->assertSame('2', $id);
    }

    /**
     * Shadow INSERT data is queryable even though lastInsertId() returns "0".
     */
    public function testShadowDataExistsDespiteZeroLastInsertId(): void
    {
        $this->pdo->exec("INSERT INTO lid_test (name, score) VALUES ('Alice', 90)");

        // lastInsertId is "0"
        $this->assertSame('0', $this->pdo->lastInsertId());

        // But the shadow data IS accessible
        $stmt = $this->pdo->query("SELECT name FROM lid_test WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Explicit ID in shadow INSERT — lastInsertId still returns "0".
     */
    public function testExplicitIdDoesNotUpdateLastInsertId(): void
    {
        $this->pdo->exec("INSERT INTO lid_test (id, name, score) VALUES (42, 'Alice', 90)");

        // Even with explicit ID, lastInsertId is "0" because no physical INSERT
        $this->assertSame('0', $this->pdo->lastInsertId());

        // But the row IS in shadow with the explicit ID
        $stmt = $this->pdo->query('SELECT id, name FROM lid_test WHERE id = 42');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(42, (int) $row['id']);
    }

    /**
     * Prepared statement INSERT also does not update lastInsertId().
     */
    public function testPreparedInsertDoesNotUpdateLastInsertId(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO lid_test (name, score) VALUES (?, ?)');
        $stmt->execute(['Alice', 90]);

        $this->assertSame('0', $this->pdo->lastInsertId());
    }

    /**
     * Physical isolation: shadow INSERT leaves physical table empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO lid_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO lid_test (id, name, score) VALUES (2, 'Bob', 80)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM lid_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
