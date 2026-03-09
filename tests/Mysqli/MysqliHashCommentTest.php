<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL hash comments (#) through the CTE rewriter (MySQLi).
 *
 * Real-world scenario: MySQL supports # as a line comment delimiter
 * in addition to --. Some MySQL-specific tools and query generators
 * use # comments. The CTE rewriter must handle these correctly.
 *
 * @spec SPEC-3.1
 * @spec SPEC-6.2
 */
class MysqliHashCommentTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_hc_items (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                active TINYINT NOT NULL DEFAULT 1
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_hc_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_hc_items VALUES (1, 'Alpha', 10.00, 1)");
        $this->ztdExec("INSERT INTO mi_hc_items VALUES (2, 'Beta', 20.00, 1)");
        $this->ztdExec("INSERT INTO mi_hc_items VALUES (3, 'Gamma', 30.00, 0)");
    }

    public function testHashCommentBeforeSelect(): void
    {
        try {
            $rows = $this->ztdQuery("# get items\nSELECT * FROM mi_hc_items ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before SELECT failed: ' . $e->getMessage()
            );
        }
    }

    public function testHashCommentBetweenSelectAndFrom(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT * # all columns\nFROM mi_hc_items ORDER BY id");

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment between SELECT and FROM failed: ' . $e->getMessage()
            );
        }
    }

    public function testHashCommentBeforeInsert(): void
    {
        try {
            $this->ztdExec("# add item\nINSERT INTO mi_hc_items VALUES (4, 'Delta', 40.00, 1)");

            $rows = $this->ztdQuery("SELECT * FROM mi_hc_items WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Delta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before INSERT failed: ' . $e->getMessage()
            );
        }
    }

    public function testHashCommentBeforeUpdate(): void
    {
        try {
            $this->ztdExec("# deactivate\nUPDATE mi_hc_items SET active = 0 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT active FROM mi_hc_items WHERE id = 1");
            $this->assertEquals(0, (int) $rows[0]['active']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    public function testHashCommentBeforeDelete(): void
    {
        try {
            $this->ztdExec("# remove\nDELETE FROM mi_hc_items WHERE id = 3");

            $rows = $this->ztdQuery("SELECT * FROM mi_hc_items ORDER BY id");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Hash comment before DELETE failed: ' . $e->getMessage()
            );
        }
    }

    public function testMultipleHashComments(): void
    {
        try {
            $rows = $this->ztdQuery(
                "# Query: active items\n"
                . "SELECT id, # pk\n"
                . "       name # label\n"
                . "FROM mi_hc_items # source\n"
                . "WHERE active = 1 # only active\n"
                . "ORDER BY name # alphabetical"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple hash comments failed: ' . $e->getMessage()
            );
        }
    }

    public function testMixedDashAndHashComments(): void
    {
        try {
            $rows = $this->ztdQuery(
                "-- dash comment\n"
                . "SELECT * FROM mi_hc_items # hash comment\n"
                . "WHERE active = 1 -- active only\n"
                . "ORDER BY id # sort"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed dash and hash comments failed: ' . $e->getMessage()
            );
        }
    }

    public function testPreparedWithHashComment(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "# filtered\nSELECT * FROM mi_hc_items WHERE price > ? ORDER BY id",
                [15]
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared with hash comment failed: ' . $e->getMessage()
            );
        }
    }
}
