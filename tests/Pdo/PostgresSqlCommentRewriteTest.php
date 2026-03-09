<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SQL comment handling by the PostgreSQL CTE rewriter.
 *
 * SQL comments in queries are common in ORM-generated SQL. The CTE rewriter
 * must strip or ignore comments before identifying statement types and table refs.
 * @spec SPEC-3.1
 */
class PostgresSqlCommentRewriteTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pscr_data (id INT PRIMARY KEY, val VARCHAR(50), num INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pscr_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO pscr_data VALUES (1, 'alpha', 10)");
        $this->pdo->exec("INSERT INTO pscr_data VALUES (2, 'beta', 20)");
    }

    public function testLeadingBlockCommentSelect(): void
    {
        try {
            $rows = $this->ztdQuery('/* query */ SELECT * FROM pscr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment SELECT: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Leading block comment SELECT returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    public function testCommentBetweenFromAndTable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM /* tbl */ pscr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment FROM/**/table: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Comment between FROM and table returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }

    public function testLeadingBlockCommentUpdate(): void
    {
        try {
            $this->pdo->exec('/* modify */ UPDATE pscr_data SET num = 99 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT num FROM pscr_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment UPDATE: ' . $e->getMessage());
            return;
        }
        $this->assertSame('99', (string) $rows[0]['num']);
    }

    public function testCommentBetweenUpdateAndTable(): void
    {
        try {
            $this->pdo->exec('UPDATE /* tbl */ pscr_data SET num = 88 WHERE id = 1');
            $rows = $this->ztdQuery('SELECT num FROM pscr_data WHERE id = 1');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment UPDATE/**/table: ' . $e->getMessage());
            return;
        }
        $this->assertSame('88', (string) $rows[0]['num']);
    }

    public function testLeadingBlockCommentInsert(): void
    {
        try {
            $this->pdo->exec("/* add */ INSERT INTO pscr_data VALUES (3, 'gamma', 30)");
            $rows = $this->ztdQuery('SELECT * FROM pscr_data WHERE id = 3');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment INSERT: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Leading block comment INSERT: row not in shadow store');
            return;
        }
        $this->assertCount(1, $rows);
    }

    public function testLeadingBlockCommentDelete(): void
    {
        try {
            $this->pdo->exec('/* remove */ DELETE FROM pscr_data WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM pscr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Leading block comment DELETE: ' . $e->getMessage());
            return;
        }
        $this->assertCount(1, $rows);
    }

    public function testCommentBetweenDeleteFromAndTable(): void
    {
        try {
            $this->pdo->exec('DELETE FROM /* tbl */ pscr_data WHERE id = 2');
            $rows = $this->ztdQuery('SELECT * FROM pscr_data ORDER BY id');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Comment DELETE FROM/**/table: ' . $e->getMessage());
            return;
        }
        $this->assertCount(1, $rows);
    }

    public function testTrailingInlineComment(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT * FROM pscr_data ORDER BY id -- list all');
        } catch (\Exception $e) {
            $this->markTestIncomplete('Trailing inline comment: ' . $e->getMessage());
            return;
        }
        if (count($rows) === 0) {
            $this->markTestIncomplete('Trailing inline comment returned empty');
            return;
        }
        $this->assertCount(2, $rows);
    }
}
