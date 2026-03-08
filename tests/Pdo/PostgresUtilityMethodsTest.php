<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests utility methods: getAvailableDrivers, lastInsertId, errorCode, errorInfo,
 * setAttribute/getAttribute on PostgreSQL via PDO.
 * @spec SPEC-4.9
 */
class PostgresUtilityMethodsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_util_test (id SERIAL PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['pg_util_test'];
    }


    public function testGetAvailableDrivers(): void
    {
        $drivers = ZtdPdo::getAvailableDrivers();
        $this->assertIsArray($drivers);
        $this->assertContains('pgsql', $drivers);
    }

    public function testLastInsertIdAfterShadowInsertThrows(): void
    {
        $this->pdo->exec("INSERT INTO pg_util_test (val) VALUES ('first')");

        // PostgreSQL lastInsertId requires sequence name and throws because
        // shadow inserts don't reach the physical DB, so the sequence is
        // never advanced ("currval of sequence is not yet defined in this session")
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessageMatches('/not yet defined/');
        $this->pdo->lastInsertId('pg_util_test_id_seq');
    }

    public function testErrorCodeAndErrorInfo(): void
    {
        $this->pdo->exec("INSERT INTO pg_util_test (val) VALUES ('test')");
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
}
