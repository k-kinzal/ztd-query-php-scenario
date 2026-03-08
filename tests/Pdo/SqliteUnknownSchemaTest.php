<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/** @spec SPEC-7.1, SPEC-7.2, SPEC-7.4 */
class SqliteUnknownSchemaTest extends TestCase
{
    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): array
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct adapter BEFORE the table exists (schema not reflected)
        $pdo = ZtdPdo::fromPdo($raw, $config);

        // Now create the table physically via the raw connection
        $raw->exec('CREATE TABLE late_table (id INTEGER PRIMARY KEY, val TEXT)');
        $raw->exec("INSERT INTO late_table VALUES (1, 'physical')");

        return [$pdo, $raw];
    }

    public function testPassthroughUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // SQLite adapter throws RuntimeException ("UPDATE simulation requires primary keys")
        // instead of passing through like MySQL/PostgreSQL adapters.
        // This is a known behavioral difference for the SQLite platform.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testPassthroughDeleteOnUnknownTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // In passthrough mode, DELETE on unknown table goes directly to the physical database
        $pdo->exec("DELETE FROM late_table WHERE id = 1");

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM late_table');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testExceptionUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // SQLite throws RuntimeException from ShadowStore rather than ZtdPdoException.
        // This differs from MySQL/PostgreSQL which throw ZtdPdoException("Unknown table").
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testExceptionDeleteOnUnknownTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // DELETE on unknown table in Exception mode throws with "Unknown table" message
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $pdo->exec("DELETE FROM late_table WHERE id = 1");
    }

    public function testSelectOnUnknownTablePassesThrough(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testInsertOnUnknownTableWorksInShadow(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO late_table (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM late_table WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
    }

    public function testEmptyResultUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        // SQLite throws RuntimeException regardless of EmptyResult mode
        // (same as Passthrough/Exception — the error comes from ShadowStore before
        // the unknown schema behavior check)
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testEmptyResultDeleteOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        // EmptyResult mode: DELETE on unknown table should return without modifying physical data
        $pdo->exec("DELETE FROM late_table WHERE id = 1");

        // Physical table should be unchanged
        $stmt = $raw->query('SELECT * FROM late_table');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testNoticeUpdateOnUnknownTableThrowsRuntimeException(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        // SQLite throws RuntimeException regardless of Notice mode
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_table SET val = 'updated' WHERE id = 1");
    }

    public function testNoticeDeleteOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered) {
            $noticeTriggered = true;
            return true;
        }, E_USER_NOTICE | E_USER_WARNING);

        try {
            $pdo->exec("DELETE FROM late_table WHERE id = 1");
        } finally {
            restore_error_handler();
        }

        $this->assertTrue($noticeTriggered);

        // Physical table should be unchanged
        $stmt = $raw->query('SELECT * FROM late_table');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
