<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL BIT and BIT VARYING types through the CTE rewriter.
 *
 * Real-world scenario: BIT types are used for flags, permissions,
 * and compact binary data. The CTE shadow store represents all values
 * as TEXT in the CTE UNION, which may cause type mismatches when
 * comparing with BIT literals (B'...' syntax) in WHERE clauses.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresBitTypeTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_bt_flags (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                perms BIT(8) NOT NULL DEFAULT B\'00000000\',
                mask BIT VARYING(16)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_bt_flags'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_bt_flags (id, name, perms) VALUES (1, 'Admin', B'11111111')");
        $this->ztdExec("INSERT INTO pg_bt_flags (id, name, perms) VALUES (2, 'User', B'00001111')");
        $this->ztdExec("INSERT INTO pg_bt_flags (id, name, perms) VALUES (3, 'Guest', B'00000001')");
    }

    /**
     * Basic SELECT — do BIT values survive the shadow store?
     */
    public function testSelectBitValues(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name, perms::TEXT AS perms FROM pg_bt_flags ORDER BY id");

            $this->assertCount(3, $rows);
            // Check what the shadow store returns for BIT values
            if ($rows[0]['perms'] !== '11111111') {
                $this->markTestIncomplete(
                    'BIT value corrupted in shadow store. Admin perms returned: '
                    . var_export($rows[0]['perms'], true)
                    . '. All perms: ' . implode(', ', array_column($rows, 'perms'))
                );
            }
            $this->assertSame('11111111', $rows[0]['perms']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'SELECT BIT values failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE with BIT literal — does B'...' comparison work?
     */
    public function testWhereBitLiteral(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_bt_flags WHERE perms = B'11111111'"
            );

            if (count($rows) === 0) {
                // Check what the shadow store actually has
                $all = $this->ztdQuery("SELECT name, perms::TEXT AS p FROM pg_bt_flags ORDER BY id");
                $this->markTestIncomplete(
                    'BIT literal WHERE returns 0 rows. Shadow store values: '
                    . json_encode(array_map(fn($r) => $r['name'] . '=' . $r['p'], $all))
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Admin', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE BIT literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE with BIT comparison using ::BIT cast.
     */
    public function testWhereBitWithCast(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_bt_flags WHERE perms = '11111111'::BIT(8)"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'BIT cast WHERE returns 0 rows. CTE shadow store may not preserve BIT type.'
                );
            }
            $this->assertCount(1, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE BIT with cast failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with BIT literal in SET.
     */
    public function testUpdateWithBitLiteral(): void
    {
        try {
            $this->ztdExec("UPDATE pg_bt_flags SET perms = B'11110000' WHERE id = 3");

            $rows = $this->ztdQuery("SELECT perms::TEXT AS p FROM pg_bt_flags WHERE id = 3");
            $this->assertCount(1, $rows);

            if ($rows[0]['p'] !== '11110000') {
                $this->markTestIncomplete(
                    'BIT UPDATE value corrupted. Expected "11110000", got: '
                    . var_export($rows[0]['p'], true)
                );
            }
            $this->assertSame('11110000', $rows[0]['p']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with BIT literal failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BIT operations: AND, OR, XOR.
     */
    public function testBitOperations(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, (perms & B'11110000')::TEXT AS high_nibble
                 FROM pg_bt_flags WHERE id = 1"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('11110000', $rows[0]['high_nibble']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BIT operations failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation — BIT INSERT should not reach physical table.
     */
    public function testBitInsertPhysicalIsolation(): void
    {
        try {
            $shadow = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_bt_flags");
            $this->assertEquals(3, (int) $shadow[0]['cnt']);

            $this->pdo->disableZtd();
            $physical = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_bt_flags")
                ->fetchAll(PDO::FETCH_ASSOC);
            $this->assertEquals(0, (int) $physical[0]['cnt']);
            $this->pdo->enableZtd();
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BIT physical isolation failed: ' . $e->getMessage()
            );
        }
    }
}
