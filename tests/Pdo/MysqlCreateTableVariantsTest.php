<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CREATE TABLE LIKE and CREATE TABLE AS SELECT on MySQL via PDO.
 * @spec SPEC-5.1b
 */
class MysqlCreateTableVariantsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_ct_source (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE mysql_ct_target_like LIKE mysql_ct_source',
            'CREATE TABLE mysql_ct_target_ctas AS SELECT * FROM mysql_ct_source',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_ct_target_like', 'mysql_ct_target_ctas', 'mysql_ct_source', 'LIKE', 'AS'];
    }


    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE mysql_ct_target_like LIKE mysql_ct_source');

        $this->pdo->exec("INSERT INTO mysql_ct_target_like (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_like WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->pdo->exec("INSERT INTO mysql_ct_source (id, val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO mysql_ct_source (id, val) VALUES (2, 'world')");

        $this->pdo->exec('CREATE TABLE mysql_ct_target_ctas AS SELECT * FROM mysql_ct_source');

        // On MySQL, CTAS works fully — SELECT from the created table returns data
        $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_ctas ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testCreateTableLikeIsolation(): void
    {
        $this->pdo->exec('CREATE TABLE mysql_ct_target_like LIKE mysql_ct_source');
        $this->pdo->exec("INSERT INTO mysql_ct_target_like (id, val) VALUES (1, 'hello')");

        $this->pdo->disableZtd();
        // Physical table should not exist
        try {
            $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_like');
            $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        } catch (\PDOException $e) {
            $this->assertStringContainsString('mysql_ct_target_like', $e->getMessage());
        }
    }
}
