<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\UserDto;

/**
 * Tests ZtdPdo::query() with fetchMode arguments.
 *
 * ZtdPdo::query() signature:
 *   query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs)
 *
 * When fetchMode is provided, it calls setFetchMode($fetchMode, ...$fetchModeArgs)
 * on the result statement. This tests the variadic args path.
 * @spec pending
 */
class MysqlQueryFetchModeArgsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_qfm_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_qfm_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_qfm_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    /**
     * query() with FETCH_ASSOC mode argument.
     */
    public function testQueryWithFetchAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertArrayNotHasKey(0, $rows[0]); // No numeric keys in FETCH_ASSOC
    }

    /**
     * query() with FETCH_NUM mode argument.
     */
    public function testQueryWithFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id', PDO::FETCH_NUM);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0][1]);
        $this->assertArrayNotHasKey('name', $rows[0]); // No string keys in FETCH_NUM
    }

    /**
     * query() with FETCH_OBJ mode argument.
     */
    public function testQueryWithFetchObj(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id', PDO::FETCH_OBJ);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertIsObject($rows[0]);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * query() with FETCH_COLUMN mode + column index argument.
     */
    public function testQueryWithFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id', PDO::FETCH_COLUMN, 1);
        $rows = $stmt->fetchAll();
        $this->assertSame(['Alice', 'Bob'], $rows);
    }

    /**
     * query() with FETCH_CLASS mode + class name argument.
     */
    public function testQueryWithFetchClass(): void
    {
        $stmt = $this->pdo->query(
            'SELECT id, name FROM pdo_qfm_test ORDER BY id',
            PDO::FETCH_CLASS,
            UserDto::class,
        );
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertInstanceOf(UserDto::class, $rows[0]);
        $this->assertSame('Alice', $rows[0]->name);
    }

    /**
     * query() without fetchMode (default behavior).
     */
    public function testQueryWithoutFetchMode(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * query() with FETCH_BOTH mode.
     */
    public function testQueryWithFetchBoth(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pdo_qfm_test ORDER BY id', PDO::FETCH_BOTH);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        // FETCH_BOTH provides both numeric and string keys
        $this->assertSame($rows[0]['name'], $rows[0][1]);
    }

    /**
     * Physical isolation: query with fetchMode still uses shadow data.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(2, (int) $count);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_qfm_test', PDO::FETCH_COLUMN, 0);
        $count = $stmt->fetch();
        $this->assertSame(0, (int) $count);
    }
}
