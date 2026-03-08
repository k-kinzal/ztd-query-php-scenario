<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/** @spec SPEC-4.2a */
class UpsertAndReplaceTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE upsert_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['upsert_test'];
    }


    public function testInsertOnDuplicateKeyUpdateInserts(): void
    {
        // When no duplicate exists, should insert
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        // Insert first
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");

        // Upsert with same PK should update
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);
    }

    public function testInsertOnDuplicateKeyUpdateIsolation(): void
    {
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        // Physical table should be empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }

    public function testReplaceIntoInserts(): void
    {
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testReplaceIntoReplacesExisting(): void
    {
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");

        // REPLACE with same PK should delete + insert
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'replaced')");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('replaced', $row['val']);

        // Only one row should exist
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testReplaceIntoIsolation(): void
    {
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'hello')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }
}
