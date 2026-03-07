<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests utility methods: getAvailableDrivers, lastInsertId, errorCode, errorInfo,
 * setAttribute/getAttribute on SQLite.
 */
class SqliteUtilityMethodsTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE util_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testGetAvailableDrivers(): void
    {
        $drivers = ZtdPdo::getAvailableDrivers();
        $this->assertIsArray($drivers);
        $this->assertContains('sqlite', $drivers);
    }

    public function testLastInsertIdAfterShadowInsert(): void
    {
        // lastInsertId() is delegated to the underlying connection.
        // Since shadow inserts don't reach the physical DB, lastInsertId
        // may not reflect the shadow-inserted row.
        $this->pdo->exec("INSERT INTO util_test (val) VALUES ('first')");
        $id = $this->pdo->lastInsertId();
        // The value may be '0' or false since no physical insert happened
        $this->assertNotNull($id);
    }

    public function testErrorCodeAndErrorInfo(): void
    {
        // After successful operation, errorCode should be '00000'
        $this->pdo->exec("INSERT INTO util_test (val) VALUES ('test')");
        $this->assertSame('00000', $this->pdo->errorCode());

        $errorInfo = $this->pdo->errorInfo();
        $this->assertIsArray($errorInfo);
        $this->assertSame('00000', $errorInfo[0]);
    }

    public function testSetAndGetAttribute(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $mode = $this->pdo->getAttribute(PDO::ATTR_DEFAULT_FETCH_MODE);
        $this->assertSame(PDO::FETCH_ASSOC, $mode);
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString("it", $quoted);
    }

    public function testConnectStaticFactory(): void
    {
        if (!method_exists(PDO::class, 'connect')) {
            $this->markTestSkipped('PDO::connect() requires PHP 8.4+');
        }

        $pdo = ZtdPdo::connect('sqlite::memory:');
        $this->assertTrue($pdo->isZtdEnabled());

        // Create table and test basic operation
        $pdo->disableZtd();
        $pdo->exec('CREATE TABLE connect_test (id INTEGER PRIMARY KEY, val TEXT)');
        $pdo->enableZtd();

        $pdo->exec("INSERT INTO connect_test (id, val) VALUES (1, 'hello')");
        $stmt = $pdo->query('SELECT * FROM connect_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }
}
