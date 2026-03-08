<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL-specific exception types for error conditions.
 *
 * MySQL's AlterTableMutation validates column existence and throws
 * ColumnAlreadyExistsException / ColumnNotFoundException, unlike SQLite
 * which silently ignores these conditions.
 * @spec pending
 */
class ExceptionTypesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ex_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE on shadow-created table that already exists should throw.
     */
    public function testCreateTableAlreadyExistsThrows(): void
    {
        $this->mysqli->query(',
            'CREATE TABLE mi_ex_new (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS on existing table should not throw.
     */
    public function testCreateTableIfNotExistsDoesNotThrow(): void
    {
        $this->mysqli->query(',
            'CREATE TABLE mi_ex_new2 (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE IF NOT EXISTS mi_ex_new2 (id INT PRIMARY KEY, val VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ex_test', 'mi_ex_new', 'mi_ex_new2', 'on'];
    }


    /**
     * CREATE TABLE on shadow-created table that already exists should throw.
     */
    public function testCreateTableAlreadyExistsThrows(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ex_new (id INT PRIMARY KEY, val VARCHAR(50))');

        $this->expectException(ZtdMysqliException::class);
        $this->mysqli->query('CREATE TABLE mi_ex_new (id INT PRIMARY KEY, val VARCHAR(50))');
    }

    /**
     * CREATE TABLE IF NOT EXISTS on existing table should not throw.
     */
    public function testCreateTableIfNotExistsDoesNotThrow(): void
    {
        $this->mysqli->query('CREATE TABLE mi_ex_new2 (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mi_ex_new2 (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->assertTrue(true);
    }

    /**
     * ALTER TABLE ADD COLUMN for existing column throws ColumnAlreadyExistsException.
     *
     * Note: This is thrown as a raw core exception, NOT wrapped in ZtdMysqliException.
     * This is an inconsistency — most other ZTD errors are wrapped.
     */
    public function testAlterTableAddExistingColumnThrows(): void
    {
        $this->expectException(\ZtdQuery\Exception\ColumnAlreadyExistsException::class);
        $this->expectExceptionMessage("Column 'name' already exists");
        $this->mysqli->query('ALTER TABLE mi_ex_test ADD COLUMN name VARCHAR(50)');
    }

    /**
     * ALTER TABLE DROP non-existent column throws ColumnNotFoundException.
     */
    public function testAlterTableDropNonExistentColumnThrows(): void
    {
        $this->expectException(\ZtdQuery\Exception\ColumnNotFoundException::class);
        $this->expectExceptionMessage("Column 'nonexistent' does not exist");
        $this->mysqli->query('ALTER TABLE mi_ex_test DROP COLUMN nonexistent');
    }

    /**
     * ALTER TABLE MODIFY non-existent column throws ColumnNotFoundException.
     */
    public function testAlterTableModifyNonExistentColumnThrows(): void
    {
        $this->expectException(\ZtdQuery\Exception\ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_ex_test MODIFY COLUMN nonexistent TEXT');
    }

    /**
     * ALTER TABLE CHANGE non-existent column throws ColumnNotFoundException.
     */
    public function testAlterTableChangeNonExistentColumnThrows(): void
    {
        $this->expectException(\ZtdQuery\Exception\ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_ex_test CHANGE COLUMN nonexistent new_name TEXT');
    }

    /**
     * ALTER TABLE RENAME non-existent column throws ColumnNotFoundException.
     */
    public function testAlterTableRenameNonExistentColumnThrows(): void
    {
        $this->expectException(\ZtdQuery\Exception\ColumnNotFoundException::class);
        $this->mysqli->query('ALTER TABLE mi_ex_test RENAME COLUMN nonexistent TO new_name');
    }
}
