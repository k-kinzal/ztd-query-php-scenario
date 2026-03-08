<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests multiple ZtdPdo instances with independent shadow stores on SQLite.
 *
 * Each ZtdPdo instance should maintain its own independent shadow store,
 * so mutations in one instance should not be visible to another.
 */
class SqliteMultipleInstancesTest extends TestCase
{
    /**
     * Two ZtdPdo instances on same database have independent shadow stores.
     */
    public function testIndependentShadowStores(): void
    {
        $raw1 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw1->exec('CREATE TABLE sl_mi_test (id INTEGER PRIMARY KEY, name TEXT)');

        // Both wrap the same underlying schema
        $pdo1 = ZtdPdo::fromPdo($raw1);

        $raw2 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw2->exec('CREATE TABLE sl_mi_test (id INTEGER PRIMARY KEY, name TEXT)');
        $pdo2 = ZtdPdo::fromPdo($raw2);

        // Insert in instance 1
        $pdo1->exec("INSERT INTO sl_mi_test VALUES (1, 'Alice')");

        // Instance 1 sees the row
        $stmt1 = $pdo1->query('SELECT COUNT(*) FROM sl_mi_test');
        $this->assertSame(1, (int) $stmt1->fetchColumn());

        // Instance 2 should NOT see instance 1's shadow data
        $stmt2 = $pdo2->query('SELECT COUNT(*) FROM sl_mi_test');
        $this->assertSame(0, (int) $stmt2->fetchColumn());
    }

    /**
     * Mutations in one instance do not affect another.
     */
    public function testMutationsIsolatedBetweenInstances(): void
    {
        $raw1 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw1->exec('CREATE TABLE sl_mi2_test (id INTEGER PRIMARY KEY, val INTEGER)');
        $pdo1 = ZtdPdo::fromPdo($raw1);

        $raw2 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw2->exec('CREATE TABLE sl_mi2_test (id INTEGER PRIMARY KEY, val INTEGER)');
        $pdo2 = ZtdPdo::fromPdo($raw2);

        $pdo1->exec('INSERT INTO sl_mi2_test VALUES (1, 100)');
        $pdo2->exec('INSERT INTO sl_mi2_test VALUES (1, 200)');

        $stmt1 = $pdo1->query('SELECT val FROM sl_mi2_test WHERE id = 1');
        $this->assertSame(100, (int) $stmt1->fetchColumn());

        $stmt2 = $pdo2->query('SELECT val FROM sl_mi2_test WHERE id = 1');
        $this->assertSame(200, (int) $stmt2->fetchColumn());
    }

    /**
     * Disabling ZTD on one instance doesn't affect the other.
     */
    public function testDisableZtdIndependent(): void
    {
        $raw1 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw1->exec('CREATE TABLE sl_mi3_test (id INTEGER PRIMARY KEY, name TEXT)');
        $pdo1 = ZtdPdo::fromPdo($raw1);

        $raw2 = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw2->exec('CREATE TABLE sl_mi3_test (id INTEGER PRIMARY KEY, name TEXT)');
        $pdo2 = ZtdPdo::fromPdo($raw2);

        $pdo1->exec("INSERT INTO sl_mi3_test VALUES (1, 'Alice')");
        $pdo2->exec("INSERT INTO sl_mi3_test VALUES (1, 'Bob')");

        // Disable ZTD on instance 1
        $pdo1->disableZtd();
        $stmt1 = $pdo1->query('SELECT COUNT(*) FROM sl_mi3_test');
        $this->assertSame(0, (int) $stmt1->fetchColumn());

        // Instance 2 still has ZTD enabled with its shadow data
        $stmt2 = $pdo2->query('SELECT name FROM sl_mi3_test WHERE id = 1');
        $this->assertSame('Bob', $stmt2->fetchColumn());
    }
}
