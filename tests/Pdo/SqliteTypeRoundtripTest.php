<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Type preservation through CTE shadow store on SQLite via PDO.
 * @spec SPEC-3.1, SPEC-4.1
 */
class SqliteTypeRoundtripTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_tr_data (id INTEGER PRIMARY KEY, int_val INTEGER, float_val REAL, str_val TEXT, nullable_val TEXT NULL)';
    }

    protected function getTableNames(): array
    {
        return ['sl_tr_data'];
    }

    public function testIntegerValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val) VALUES (1, 0)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val) VALUES (2, 1)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val) VALUES (3, -1)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val) VALUES (4, 2147483647)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val) VALUES (5, 9999999999)");

        $rows = $this->ztdQuery('SELECT id, int_val FROM sl_tr_data ORDER BY id');
        $this->assertSame(0, (int) $rows[0]['int_val']);
        $this->assertSame(1, (int) $rows[1]['int_val']);
        $this->assertSame(-1, (int) $rows[2]['int_val']);
        $this->assertSame(2147483647, (int) $rows[3]['int_val']);
        $this->assertSame(9999999999, (int) $rows[4]['int_val']);
    }

    public function testFloatValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, float_val) VALUES (1, 0.0)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, float_val) VALUES (2, 3.14159)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, float_val) VALUES (3, -0.001)");

        $rows = $this->ztdQuery('SELECT float_val FROM sl_tr_data ORDER BY id');
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(3.14159, (float) $rows[1]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(-0.001, (float) $rows[2]['float_val'], 0.0001);
    }

    public function testStringValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, str_val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, str_val) VALUES (2, '')");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, str_val) VALUES (3, 'It''s a test')");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, str_val) VALUES (4, '  spaces  ')");

        $rows = $this->ztdQuery('SELECT str_val FROM sl_tr_data ORDER BY id');
        $this->assertSame('hello', $rows[0]['str_val']);
        $this->assertSame('', $rows[1]['str_val']);
        $this->assertSame("It's a test", $rows[2]['str_val']);
        $this->assertSame('  spaces  ', $rows[3]['str_val']);
    }

    public function testNullValues(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, nullable_val) VALUES (2, 'not null')");

        $rows = $this->ztdQuery('SELECT nullable_val FROM sl_tr_data ORDER BY id');
        $this->assertNull($rows[0]['nullable_val']);
        $this->assertSame('not null', $rows[1]['nullable_val']);
    }

    public function testNullFiltering(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->pdo->exec("INSERT INTO sl_tr_data (id, nullable_val) VALUES (2, 'val')");

        $isNull = $this->ztdQuery('SELECT id FROM sl_tr_data WHERE nullable_val IS NULL');
        $this->assertCount(1, $isNull);
        $this->assertSame(1, (int) $isNull[0]['id']);

        $notNull = $this->ztdQuery('SELECT id FROM sl_tr_data WHERE nullable_val IS NOT NULL');
        $this->assertCount(1, $notNull);
        $this->assertSame(2, (int) $notNull[0]['id']);
    }

    public function testUpdatePreservesType(): void
    {
        $this->pdo->exec("INSERT INTO sl_tr_data (id, int_val, str_val) VALUES (1, 10, 'original')");
        $this->pdo->exec("UPDATE sl_tr_data SET int_val = 20, str_val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT int_val, str_val FROM sl_tr_data WHERE id = 1');
        $this->assertSame(20, (int) $rows[0]['int_val']);
        $this->assertSame('updated', $rows[0]['str_val']);
    }

    public function testPreparedTypeRoundtrip(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO sl_tr_data (id, int_val, float_val, str_val) VALUES (?, ?, ?, ?)");
        $stmt->execute([1, 42, 3.14, 'prepared']);

        $rows = $this->ztdQuery('SELECT int_val, float_val, str_val FROM sl_tr_data WHERE id = 1');
        $this->assertSame(42, (int) $rows[0]['int_val']);
        $this->assertEqualsWithDelta(3.14, (float) $rows[0]['float_val'], 0.001);
        $this->assertSame('prepared', $rows[0]['str_val']);
    }

    public function testLargeTextValue(): void
    {
        $longText = str_repeat('abcde', 200);
        $this->pdo->exec("INSERT INTO sl_tr_data (id, str_val) VALUES (1, '{$longText}')");

        $rows = $this->ztdQuery('SELECT str_val FROM sl_tr_data WHERE id = 1');
        $this->assertSame($longText, $rows[0]['str_val']);
    }
}
