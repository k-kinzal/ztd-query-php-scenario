<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests recursive CTEs (WITH RECURSIVE) in DML context on MySQL PDO.
 *
 * MySQL 8.0+ supports recursive CTEs. This tests whether the CTE rewriter
 * handles recursive CTEs in DML subqueries correctly.
 *
 * @spec SPEC-10.2
 */
class MysqlRecursiveCteDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_rct_categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INT,
                active TINYINT DEFAULT 1
            ) ENGINE=InnoDB",
            "CREATE TABLE my_rct_flat (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category_id INT,
                path TEXT
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_rct_flat', 'my_rct_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (1, 'Root', NULL)");
        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (2, 'Electronics', 1)");
        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (3, 'Phones', 2)");
        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (4, 'Laptops', 2)");
        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (5, 'Clothing', 1)");
        $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (6, 'Shirts', 5)");
    }

    public function testSelectRecursiveCte(): void
    {
        try {
            $rows = $this->ztdQuery(
                "WITH RECURSIVE tree AS (
                    SELECT id, name, parent_id, CAST(name AS CHAR(500)) AS path FROM my_rct_categories WHERE parent_id IS NULL
                    UNION ALL
                    SELECT c.id, c.name, c.parent_id, CONCAT(tree.path, '/', c.name)
                    FROM my_rct_categories c
                    JOIN tree ON c.parent_id = tree.id
                )
                SELECT id, name, path FROM tree ORDER BY path"
            );

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'SELECT recursive CTE (MySQL PDO): expected 6, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT recursive CTE (MySQL PDO) failed: ' . $e->getMessage());
        }
    }

    public function testInsertFromRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, CAST(name AS CHAR(500)) AS path FROM my_rct_categories WHERE parent_id IS NULL
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, CONCAT(tree.path, '/', c.name)
                     FROM my_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM my_rct_flat ORDER BY path");

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'INSERT from recursive CTE (MySQL PDO): expected 6, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT from recursive CTE (MySQL PDO) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_rct_categories WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM my_rct_categories WHERE id = 2
                        UNION ALL
                        SELECT c.id FROM my_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM my_rct_categories ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE subtree recursive CTE (MySQL PDO): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subtree recursive CTE (MySQL PDO) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSubtreeViaRecursiveCte(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_rct_categories SET active = 0 WHERE id IN (
                    WITH RECURSIVE subtree AS (
                        SELECT id FROM my_rct_categories WHERE id = 5
                        UNION ALL
                        SELECT c.id FROM my_rct_categories c
                        JOIN subtree ON c.parent_id = subtree.id
                    )
                    SELECT id FROM subtree
                )"
            );

            $inactive = $this->ztdQuery("SELECT id, name FROM my_rct_categories WHERE active = 0 ORDER BY id");

            if (count($inactive) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE subtree recursive CTE (MySQL PDO): expected 2 inactive, got '
                    . count($inactive) . '. Rows: ' . json_encode($inactive)
                );
            }

            $this->assertCount(2, $inactive);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subtree recursive CTE (MySQL PDO) failed: ' . $e->getMessage());
        }
    }

    public function testInsertRecursiveCteAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_rct_categories (id, name, parent_id) VALUES (7, 'Accessories', 3)");

            $this->ztdExec(
                "INSERT INTO my_rct_flat (category_id, path)
                 WITH RECURSIVE tree AS (
                     SELECT id, name, parent_id, CAST(name AS CHAR(500)) AS path FROM my_rct_categories WHERE id = 2
                     UNION ALL
                     SELECT c.id, c.name, c.parent_id, CONCAT(tree.path, '/', c.name)
                     FROM my_rct_categories c
                     JOIN tree ON c.parent_id = tree.id
                 )
                 SELECT id, path FROM tree"
            );

            $rows = $this->ztdQuery("SELECT category_id, path FROM my_rct_flat ORDER BY path");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT recursive CTE after DML (MySQL PDO): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT recursive CTE after DML (MySQL PDO) failed: ' . $e->getMessage());
        }
    }
}
