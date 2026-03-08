<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests utility methods: getAvailableDrivers, lastInsertId, errorCode, errorInfo,
 * setAttribute/getAttribute on MySQL via PDO.
 * @spec SPEC-4.9
 */
class MysqlUtilityMethodsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_util_test (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_util_test'];
    }


    public function testGetAvailableDrivers(): void
    {
        $drivers = ZtdPdo::getAvailableDrivers();
        $this->assertIsArray($drivers);
        $this->assertContains('mysql', $drivers);
    }

    public function testLastInsertIdAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO mysql_util_test (val) VALUES ('first')");
        $id = $this->pdo->lastInsertId();
        // lastInsertId is delegated; may not reflect shadow insert
        $this->assertNotNull($id);
    }

    public function testErrorCodeAndErrorInfo(): void
    {
        $this->pdo->exec("INSERT INTO mysql_util_test (val) VALUES ('test')");
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

        $pdo = ZtdPdo::connect(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->assertTrue($pdo->isZtdEnabled());

        $pdo->exec("INSERT INTO mysql_util_test (val) VALUES ('connected')");
        $stmt = $pdo->query("SELECT val FROM mysql_util_test WHERE val = 'connected'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('connected', $rows[0]['val']);
    }
}
