<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests unknown schema behavior workflows: INSERT into unreflected table,
 * then UPDATE/DELETE the inserted rows, across all 4 behavior modes.
 * Exposes platform inconsistencies documented in SPEC-7.1–7.4.
 * @spec SPEC-7.1, SPEC-7.2, SPEC-7.3, SPEC-7.4
 */
class SqliteUnknownSchemaWorkflowTest extends TestCase
{
    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): array
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);
        $pdo = ZtdPdo::fromPdo($raw, $config);

        // Create table AFTER adapter construction (schema not reflected)
        $raw->exec('CREATE TABLE late_wf (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)');
        $raw->exec("INSERT INTO late_wf VALUES (1, 'physical', 10)");

        return [$pdo, $raw];
    }

    /**
     * Passthrough: INSERT into unreflected table creates shadow data,
     * then UPDATE on that shadow data throws RuntimeException on SQLite.
     */
    public function testPassthroughInsertThenUpdateThrowsOnSqlite(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // INSERT works — creates shadow data for the unreflected table
        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // SELECT sees shadow row
        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // UPDATE on unreflected table throws RuntimeException
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * Passthrough: INSERT registers table in shadow store, so subsequent DELETE
     * operates on shadow data (not passthrough to physical DB).
     */
    public function testPassthroughInsertThenDeleteOperatesOnShadow(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // After INSERT, table is registered in shadow store.
        // DELETE now operates on shadow data, not passthrough.
        $pdo->exec("DELETE FROM late_wf WHERE id = 2");

        // Shadow row deleted
        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);

        // Physical table unchanged (DELETE went to shadow, not physical)
        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Exception: INSERT works but UPDATE on unreflected table throws.
     */
    public function testExceptionInsertThenUpdateThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);

        // UPDATE throws RuntimeException (not ZtdPdoException) on SQLite
        $this->expectException(\RuntimeException::class);
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * Exception: After INSERT, DELETE behavior depends on whether the shadow
     * store registered the table. May throw "Unknown table" or succeed.
     */
    public function testExceptionInsertThenDeleteBehavior(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        try {
            $pdo->exec("DELETE FROM late_wf WHERE id = 2");
            // If DELETE succeeds, the INSERT registered the table in shadow store
            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(0, $rows, 'DELETE removed shadow row');
        } catch (\RuntimeException $e) {
            // If DELETE throws, Exception mode prevents schema registration
            $this->assertMatchesRegularExpression('/unknown table/i', $e->getMessage());
        }
    }

    /**
     * EmptyResult: INSERT works, UPDATE throws despite EmptyResult mode.
     */
    public function testEmptyResultInsertThenUpdateThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);

        // EmptyResult mode ignored for UPDATE on SQLite — throws RuntimeException
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * EmptyResult: INSERT then DELETE returns empty (physical unchanged).
     */
    public function testEmptyResultInsertThenDelete(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // DELETE returns without modifying physical data
        $pdo->exec("DELETE FROM late_wf WHERE id = 1");

        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Notice: INSERT works, UPDATE throws despite Notice mode.
     */
    public function testNoticeInsertThenUpdateThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // Notice mode ignored for UPDATE on SQLite — throws RuntimeException
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * Notice: INSERT registers table, so DELETE operates on shadow data
     * without triggering notice (table is now known).
     */
    public function testNoticeInsertThenDeleteOperatesOnShadow(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // After INSERT, table is known. DELETE operates on shadow without notice.
        $pdo->exec("DELETE FROM late_wf WHERE id = 2");

        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);

        // Physical table unchanged
        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * SELECT on unreflected table always passes through to physical DB,
     * regardless of behavior mode.
     */
    public function testSelectAlwaysPassesThroughAllModes(): void
    {
        foreach (UnknownSchemaBehavior::cases() as $behavior) {
            [$pdo] = $this->createAdapterThenTable($behavior);

            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows, "SELECT should pass through in {$behavior->name} mode");
            $this->assertSame('physical', $rows[0]['val']);
        }
    }

    /**
     * Prepared SELECT on unreflected table passes through.
     */
    public function testPreparedSelectOnUnreflectedTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $stmt = $pdo->prepare('SELECT val FROM late_wf WHERE id = ?');
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    /**
     * Mixed: reflected table + unreflected table in same session.
     */
    public function testMixedReflectedAndUnreflected(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        // Create "known" table BEFORE adapter construction
        $raw->exec('CREATE TABLE known_tbl (id INTEGER PRIMARY KEY, val TEXT)');

        $config = new ZtdConfig(unknownSchemaBehavior: UnknownSchemaBehavior::Passthrough);
        $pdo = ZtdPdo::fromPdo($raw, $config);

        // Create "late" table AFTER adapter construction
        $raw->exec('CREATE TABLE late_tbl (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO late_tbl VALUES (1, 'physical')");

        // Shadow INSERT into known table
        $pdo->exec("INSERT INTO known_tbl (id, val) VALUES (1, 'shadow')");

        // Shadow data visible for reflected table
        $rows = $pdo->query('SELECT val FROM known_tbl WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // Physical data visible for unreflected table
        $rows = $pdo->query('SELECT val FROM late_tbl WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }
}
