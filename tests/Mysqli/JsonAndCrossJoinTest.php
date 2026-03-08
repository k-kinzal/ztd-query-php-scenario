<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests JSON data handling and CROSS JOIN patterns on MySQLi.
 * @spec pending
 */
class JsonAndCrossJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_jcj_products (id INT PRIMARY KEY, name VARCHAR(255), metadata JSON)',
            'CREATE TABLE mi_jcj_colors (id INT PRIMARY KEY, color VARCHAR(50))',
            'CREATE TABLE mi_jcj_sizes (id INT PRIMARY KEY, size VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_jcj_products', 'mi_jcj_colors', 'mi_jcj_sizes'];
    }


    public function testInsertAndSelectJsonData(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");

        $result = $this->mysqli->query('SELECT name, metadata FROM mi_jcj_products ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);

        $meta1 = json_decode($rows[0]['metadata'], true);
        $this->assertSame('red', $meta1['color']);
    }

    public function testJsonExtractFunction(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\"}')");
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\"}')");

        $result = $this->mysqli->query("SELECT name, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM mi_jcj_products ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('red', $rows[0]['color']);
        $this->assertSame('blue', $rows[1]['color']);
    }

    public function testCrossJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'Small')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'Medium')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (3, 'Large')");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c
            CROSS JOIN mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(6, $rows);
    }

    public function testImplicitCrossJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'M')");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c, mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testCrossJoinAfterMutations(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'M')");

        $this->mysqli->query("DELETE FROM mi_jcj_colors WHERE id = 2");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c
            CROSS JOIN mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Red', $rows[0]['color']);
    }
}
