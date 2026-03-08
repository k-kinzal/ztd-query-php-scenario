<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CREATE TABLE IF NOT EXISTS on MySQL PDO.
 * @spec SPEC-5.1b
 */
class MysqlCreateTableIfNotExistsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_mctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mctine_test VALUES (1,',
            'CREATE TABLE IF NOT EXISTS pdo_mctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->pdo->exec(',
            'CREATE TABLE IF NOT EXISTS pdo_mctine_new (id INT PRIMARY KEY, val VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_mctine_test', 'pdo_mctine_new', 'on', 'creates'];
    }


    /**
     * CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mctine_test VALUES (1, 'Alice')");

        $this->pdo->exec('CREATE TABLE IF NOT EXISTS pdo_mctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        $stmt = $this->pdo->query('SELECT name FROM pdo_mctine_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS pdo_mctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->pdo->exec("INSERT INTO pdo_mctine_new VALUES (1, 'test')");

        $stmt = $this->pdo->query('SELECT val FROM pdo_mctine_new WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }
}
