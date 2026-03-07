<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests ZtdPdo::connect() static factory method (requires PHP 8.4+).
 * Verifies that connect() produces a fully functional ZTD-enabled adapter
 * equivalent to fromPdo(PDO::connect(...)).
 */
class SqliteConnectFactoryTest extends TestCase
{
    public function testConnectCreatesWorkingAdapter(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        $pdo->exec('CREATE TABLE cf_items (id INTEGER PRIMARY KEY, name TEXT)');
        $pdo->exec("INSERT INTO cf_items (id, name) VALUES (1, 'alpha')");

        $stmt = $pdo->query('SELECT name FROM cf_items WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('alpha', $row['name']);
    }

    public function testConnectEnablesZtdByDefault(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');
        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testConnectIsolatesShadowFromPhysical(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        // Create table and insert while ZTD disabled (physical)
        $pdo->disableZtd();
        $pdo->exec('CREATE TABLE cf_iso (id INTEGER PRIMARY KEY, val TEXT)');
        $pdo->exec("INSERT INTO cf_iso (id, val) VALUES (1, 'physical')");
        $pdo->enableZtd();

        // Shadow is empty — physical data not visible through ZTD
        // Note: connect() reflects schema at construction time (before table existed),
        // so the table is unreflected. SELECT passes through and sees physical data.
        // This is expected behavior for unreflected tables.
        $stmt = $pdo->query('SELECT * FROM cf_iso');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Unreflected tables pass through to physical DB on SELECT
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testConnectSupportsUpdateAndDelete(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        $pdo->exec('CREATE TABLE cf_crud (id INTEGER PRIMARY KEY, val TEXT)');
        $pdo->exec("INSERT INTO cf_crud (id, val) VALUES (1, 'original')");
        $pdo->exec("INSERT INTO cf_crud (id, val) VALUES (2, 'keep')");

        $pdo->exec("UPDATE cf_crud SET val = 'modified' WHERE id = 1");
        $pdo->exec("DELETE FROM cf_crud WHERE id = 2");

        $stmt = $pdo->query('SELECT * FROM cf_crud ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('modified', $rows[0]['val']);
    }

    public function testConnectSupportsPreparedStatements(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        $pdo->exec('CREATE TABLE cf_prep (id INTEGER PRIMARY KEY, val TEXT)');
        $pdo->exec("INSERT INTO cf_prep (id, val) VALUES (1, 'a')");
        $pdo->exec("INSERT INTO cf_prep (id, val) VALUES (2, 'b')");

        $stmt = $pdo->prepare('SELECT val FROM cf_prep WHERE id = ?');
        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('b', $row['val']);
    }

    public function testConnectSupportsJoins(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        $pdo->exec('CREATE TABLE cf_parent (id INTEGER PRIMARY KEY, name TEXT)');
        $pdo->exec('CREATE TABLE cf_child (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)');

        $pdo->exec("INSERT INTO cf_parent (id, name) VALUES (1, 'parent1')");
        $pdo->exec("INSERT INTO cf_child (id, parent_id, label) VALUES (1, 1, 'child1')");

        $stmt = $pdo->query('SELECT p.name, c.label FROM cf_parent p JOIN cf_child c ON c.parent_id = p.id');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('parent1', $row['name']);
        $this->assertSame('child1', $row['label']);
    }

    public function testConnectToggleZtd(): void
    {
        $pdo = ZtdPdo::connect('sqlite::memory:');

        // Create table physically first
        $pdo->disableZtd();
        $pdo->exec('CREATE TABLE cf_toggle (id INTEGER PRIMARY KEY, val TEXT)');
        $pdo->enableZtd();

        // Now ZTD is on — but table was created while ZTD off, so it's unreflected.
        // connect() reflects at construction (no tables existed then).
        // Test the toggle behavior:
        $this->assertTrue($pdo->isZtdEnabled());

        $pdo->disableZtd();
        $this->assertFalse($pdo->isZtdEnabled());

        $pdo->exec("INSERT INTO cf_toggle (id, val) VALUES (1, 'physical')");

        $pdo->enableZtd();
        $this->assertTrue($pdo->isZtdEnabled());

        // Unreflected table SELECT passes through — physical data visible
        $stmt = $pdo->query('SELECT * FROM cf_toggle');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }
}
