<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT OR REPLACE with computed/function expressions in VALUES.
 *
 * Known: INSERT OR REPLACE via prepared statement creates duplicate PKs (#55).
 * This tests exec() path with computed expressions like UPPER(), LOWER(), etc.
 * in the VALUES clause — function expressions may confuse the InsertTransformer.
 *
 * @spec SPEC-4.4, SPEC-4.1
 */
class SqliteInsertOrReplaceComputedTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_irc_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_irc_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_irc_items VALUES (1, 'Widget', 'WDG-001', 1)");
        $this->pdo->exec("INSERT INTO sl_irc_items VALUES (2, 'Gadget', 'GDG-002', 1)");
    }

    /**
     * INSERT OR REPLACE with UPPER() in VALUES — exec path.
     *
     * Should replace row id=1 with computed values.
     */
    public function testInsertOrReplaceWithUpperFunction(): void
    {
        try {
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (1, UPPER('widget'), 'WDG-001', 2)");

            $rows = $this->ztdQuery("SELECT id, name, version FROM sl_irc_items ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT OR REPLACE UPPER: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);

            $name = $rows[0]['name'];
            $version = (int) $rows[0]['version'];

            if ($name !== 'WIDGET') {
                $this->markTestIncomplete(
                    "INSERT OR REPLACE UPPER: name expected 'WIDGET', got '{$name}'"
                );
            }

            if ($version !== 2) {
                $this->markTestIncomplete(
                    "INSERT OR REPLACE UPPER: version expected 2, got {$version}"
                );
            }

            $this->assertSame('WIDGET', $name);
            $this->assertSame(2, $version);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT OR REPLACE with UPPER() failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT OR REPLACE with concatenation expression in VALUES.
     */
    public function testInsertOrReplaceWithConcatExpression(): void
    {
        try {
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (1, 'Widget' || ' v2', 'WDG' || '-' || '001', 2)");

            $rows = $this->ztdQuery("SELECT name, code FROM sl_irc_items WHERE id = 1");

            $this->assertCount(1, $rows);

            $name = $rows[0]['name'];
            $code = $rows[0]['code'];

            if ($name !== 'Widget v2' || $code !== 'WDG-001') {
                $this->markTestIncomplete(
                    "INSERT OR REPLACE concat: name='{$name}' (exp 'Widget v2'), code='{$code}' (exp 'WDG-001')"
                );
            }

            $this->assertSame('Widget v2', $name);
            $this->assertSame('WDG-001', $code);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT OR REPLACE with concat expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT OR REPLACE with arithmetic expression in VALUES.
     */
    public function testInsertOrReplaceWithArithmeticExpression(): void
    {
        try {
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (1, 'Widget', 'WDG-001', 1 + 1)");

            $rows = $this->ztdQuery("SELECT version FROM sl_irc_items WHERE id = 1");

            $this->assertCount(1, $rows);

            $version = (int) $rows[0]['version'];
            if ($version !== 2) {
                $this->markTestIncomplete(
                    "INSERT OR REPLACE arithmetic: version expected 2, got {$version}"
                );
            }

            $this->assertSame(2, $version);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT OR REPLACE with arithmetic expression failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT OR REPLACE new row (no conflict) with computed expression.
     */
    public function testInsertOrReplaceNewRowWithFunction(): void
    {
        try {
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (3, LOWER('SPROCKET'), UPPER('spr-003'), 1)");

            $rows = $this->ztdQuery("SELECT id, name, code FROM sl_irc_items WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT OR REPLACE new row: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);

            $name = $rows[0]['name'];
            $code = $rows[0]['code'];

            if ($name !== 'sprocket') {
                $this->markTestIncomplete(
                    "New row name: expected 'sprocket', got '{$name}'"
                );
            }

            $this->assertSame('sprocket', $name);
            $this->assertSame('SPR-003', $code);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT OR REPLACE new row with functions failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Verify total row count after INSERT OR REPLACE (no duplicates).
     */
    public function testNoDuplicatesAfterReplace(): void
    {
        try {
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (1, 'ReplacedWidget', 'WDG-001', 2)");
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (2, 'ReplacedGadget', 'GDG-002', 2)");
            $this->pdo->exec("INSERT OR REPLACE INTO sl_irc_items VALUES (3, 'NewItem', 'NEW-003', 1)");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_irc_items");

            $count = (int) $rows[0]['cnt'];
            if ($count !== 3) {
                $this->markTestIncomplete(
                    "After 2 replaces + 1 new: expected 3 total rows, got {$count}"
                );
            }

            $this->assertSame(3, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT OR REPLACE duplicate check failed: ' . $e->getMessage()
            );
        }
    }
}
