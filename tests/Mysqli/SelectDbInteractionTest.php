<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests select_db() interaction with ZTD on MySQLi.
 *
 * ZtdMysqli::select_db() delegates directly to the inner mysqli.
 * This changes the physical connection's active database, but the
 * ZTD session's schema reflection and shadow store are NOT updated.
 *
 * This can cause subtle issues:
 * - Tables in the new database may not be reflected in ZTD
 * - Shadow data from the original database persists
 * - Schema reflection uses the original database context
 * @spec SPEC-2.1
 */
class SelectDbInteractionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sdb_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE test_alt.sdb_alt_test (id INT PRIMARY KEY, val VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sdb_test', 'test_alt'];
    }


    /**
     * select_db() returns true on success.
     */
    public function testSelectDbReturnsTrue(): void
    {
        $result = $this->mysqli->select_db('test_alt');
        $this->assertTrue($result);
    }

    /**
     * Shadow data from original database persists after select_db.
     */
    public function testShadowDataPersistsAfterSelectDb(): void
    {
        // Insert into shadow on original database
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Alice')");

        // Switch database
        $this->mysqli->select_db('test_alt');

        // Switch back
        $this->mysqli->select_db('test');

        // Shadow data should still be there
        $result = $this->mysqli->query('SELECT name FROM sdb_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * After select_db, queries on original db table still work via shadow.
     */
    public function testQueriesOnOriginalTableWorkAfterSelectDb(): void
    {
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Alice')");

        // Switch to alt database
        $this->mysqli->select_db('test_alt');

        // Querying the original table may still work if ZTD has it reflected.
        // The CTE rewrite uses the shadow data regardless of active database.
        // However, the physical SELECT part of the CTE runs against the current database.
        // This tests the actual behavior — it may succeed or fail depending on
        // whether the table is accessible cross-database.
        $result = $this->mysqli->query('SELECT name FROM sdb_test WHERE id = 1');
        // The CTE rewrite constructs the shadow data as SELECT ... UNION ALL ...
        // The table reference in the CTE should still resolve since ZTD has it reflected
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Physical isolation: select_db changes physical context only.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Shadow')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM sdb_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * select_db throws exception for non-existent database.
     */
    public function testSelectDbThrowsForBadDb(): void
    {
        $this->expectException(\mysqli_sql_exception::class);
        $this->mysqli->select_db('nonexistent_db_12345');
    }
}
