<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TABLE IF NOT EXISTS behavior on SQLite with ZTD.
 * @spec SPEC-5.1b
 */
class SqliteCreateTableIfNotExistsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS on existing table does nothing.
     */
    public function testCreateIfNotExistsOnExistingTable(): void
    {
        $this->pdo->exec("INSERT INTO ctine_test VALUES (1,',
            'CREATE TABLE IF NOT EXISTS ctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS on new table creates it.
     */
    public function testCreateIfNotExistsOnNewTable(): void
    {
        $this->pdo->exec(',
            'CREATE TABLE IF NOT EXISTS ctine_new (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE without IF NOT EXISTS on existing table throws.
     */
    public function testCreateWithoutIfNotExistsOnExistingThrows(): void
    {
        $this->expectException(\\Throwable::class);
        $this->pdo->exec(',
        ];
    }

    protected function getTableNames(): array
    {
        return ['behavior', 'ctine_test', 'on', 'ctine_new', 'without'];
    }


    /**
     * CREATE TABLE IF NOT EXISTS on existing table does nothing.
     */
    public function testCreateIfNotExistsOnExistingTable(): void
    {
        $this->pdo->exec("INSERT INTO ctine_test VALUES (1, 'Alice')");

        // Should not error on existing table
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        // Data still accessible
        $stmt = $this->pdo->query('SELECT name FROM ctine_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE IF NOT EXISTS on new table creates it.
     */
    public function testCreateIfNotExistsOnNewTable(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ctine_new VALUES (1, 'test')");

        $stmt = $this->pdo->query('SELECT val FROM ctine_new WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE without IF NOT EXISTS on existing table throws.
     */
    public function testCreateWithoutIfNotExistsOnExistingThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('CREATE TABLE ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }
}
