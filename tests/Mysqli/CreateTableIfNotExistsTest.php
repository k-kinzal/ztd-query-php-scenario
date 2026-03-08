<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CREATE TABLE IF NOT EXISTS via MySQLi.
 *
 * Cross-platform parity with MysqlCreateTableIfNotExistsTest (PDO).
 * @spec SPEC-5.1b
 */
class CreateTableIfNotExistsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_ctine_test VALUES (1,',
            'CREATE TABLE IF NOT EXISTS mi_ctine_test (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->mysqli->query(',
            'CREATE TABLE IF NOT EXISTS mi_ctine_new (id INT PRIMARY KEY, val VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ctine_test', 'mi_ctine_new', 'via', 'on', 'creates'];
    }


    /**
     * CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_ctine_test VALUES (1, 'Alice')");

        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mi_ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        $result = $this->mysqli->query('SELECT name FROM mi_ctine_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mi_ctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_ctine_new VALUES (1, 'test')");

        $result = $this->mysqli->query('SELECT val FROM mi_ctine_new WHERE id = 1');
        $this->assertSame('test', $result->fetch_assoc()['val']);
    }
}
