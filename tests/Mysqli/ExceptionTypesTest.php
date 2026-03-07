<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

/**
 * Tests MySQL-specific exception types for error conditions.
 *
 * MySQL's AlterTableMutation validates column existence and throws
 * ColumnAlreadyExistsException / ColumnNotFoundException, unlike SQLite
 * which silently ignores these conditions.
 */
class ExceptionTypesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_ex_test');
        $raw->query('CREATE TABLE mi_ex_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
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

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_ex_test');
            $raw->query('DROP TABLE IF EXISTS mi_ex_new');
            $raw->query('DROP TABLE IF EXISTS mi_ex_new2');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
