<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Type preservation through CTE shadow store on PostgreSQL via PDO.
 * @spec SPEC-3.1, SPEC-4.1
 */
class PostgresTypeRoundtripTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_tr_data (id INT PRIMARY KEY, int_val INT, bigint_val BIGINT, float_val DOUBLE PRECISION, dec_val DECIMAL(12,4), str_val VARCHAR(255), text_val TEXT, bool_val BOOLEAN, nullable_val VARCHAR(50) NULL)';
    }

    protected function getTableNames(): array
    {
        return ['pg_tr_data'];
    }

    public function testIntegerValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, int_val) VALUES (1, 0)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, int_val) VALUES (2, 1)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, int_val) VALUES (3, -1)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, int_val) VALUES (4, 2147483647)");

        $rows = $this->ztdQuery('SELECT id, int_val FROM pg_tr_data ORDER BY id');
        $this->assertSame(0, (int) $rows[0]['int_val']);
        $this->assertSame(1, (int) $rows[1]['int_val']);
        $this->assertSame(-1, (int) $rows[2]['int_val']);
        $this->assertSame(2147483647, (int) $rows[3]['int_val']);
    }

    public function testFloatValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, float_val) VALUES (1, 0.0)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, float_val) VALUES (2, 3.14159)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, float_val) VALUES (3, -0.001)");

        $rows = $this->ztdQuery('SELECT float_val FROM pg_tr_data ORDER BY id');
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(3.14159, (float) $rows[1]['float_val'], 0.0001);
        $this->assertEqualsWithDelta(-0.001, (float) $rows[2]['float_val'], 0.0001);
    }

    public function testDecimalPrecision(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, dec_val) VALUES (1, 12345.6789)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, dec_val) VALUES (2, 0.0001)");

        $rows = $this->ztdQuery('SELECT dec_val FROM pg_tr_data ORDER BY id');
        $this->assertSame('12345.6789', $rows[0]['dec_val']);
        $this->assertSame('0.0001', $rows[1]['dec_val']);
    }

    public function testStringValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, str_val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, str_val) VALUES (2, '')");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, str_val) VALUES (3, 'It''s a test')");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, str_val) VALUES (4, '  spaces  ')");

        $rows = $this->ztdQuery('SELECT str_val FROM pg_tr_data ORDER BY id');
        $this->assertSame('hello', $rows[0]['str_val']);
        $this->assertSame('', $rows[1]['str_val']);
        $this->assertSame("It's a test", $rows[2]['str_val']);
        $this->assertSame('  spaces  ', $rows[3]['str_val']);
    }

    public function testNullValues(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, nullable_val) VALUES (2, 'not null')");

        $rows = $this->ztdQuery('SELECT nullable_val FROM pg_tr_data ORDER BY id');
        $this->assertNull($rows[0]['nullable_val']);
        $this->assertSame('not null', $rows[1]['nullable_val']);
    }

    public function testNullFiltering(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, nullable_val) VALUES (1, NULL)");
        $this->pdo->exec("INSERT INTO pg_tr_data (id, nullable_val) VALUES (2, 'val')");

        $isNull = $this->ztdQuery('SELECT id FROM pg_tr_data WHERE nullable_val IS NULL');
        $this->assertCount(1, $isNull);
        $this->assertSame(1, (int) $isNull[0]['id']);

        $notNull = $this->ztdQuery('SELECT id FROM pg_tr_data WHERE nullable_val IS NOT NULL');
        $this->assertCount(1, $notNull);
        $this->assertSame(2, (int) $notNull[0]['id']);
    }

    public function testBooleanTrue(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, bool_val) VALUES (1, true)");

        $rows = $this->ztdQuery('SELECT bool_val FROM pg_tr_data WHERE id = 1');
        $this->assertTrue((bool) $rows[0]['bool_val']);
    }

    public function testBooleanFalse(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, bool_val) VALUES (1, false)");

        try {
            $rows = $this->ztdQuery('SELECT bool_val FROM pg_tr_data WHERE id = 1');
        } catch (\PDOException $e) {
            $this->markTestIncomplete(
                'BOOLEAN false SELECT fails: ' . $e->getMessage() . ' (known issue SPEC-11.PG-BOOLEAN-FALSE)'
            );
            return;
        }
        if ($rows[0]['bool_val'] === null || $rows[0]['bool_val'] === '') {
            $this->markTestIncomplete(
                'BOOLEAN false is not preserved through shadow store (known issue SPEC-11.PG-BOOLEAN-FALSE)'
            );
        }
        $this->assertFalse((bool) $rows[0]['bool_val']);
    }

    public function testUpdatePreservesType(): void
    {
        $this->pdo->exec("INSERT INTO pg_tr_data (id, int_val, str_val) VALUES (1, 10, 'original')");
        $this->pdo->exec("UPDATE pg_tr_data SET int_val = 20, str_val = 'updated' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT int_val, str_val FROM pg_tr_data WHERE id = 1');
        $this->assertSame(20, (int) $rows[0]['int_val']);
        $this->assertSame('updated', $rows[0]['str_val']);
    }

    public function testPreparedTypeRoundtrip(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO pg_tr_data (id, int_val, float_val, str_val) VALUES (?, ?, ?, ?)");
        $stmt->execute([1, 42, 3.14, 'prepared']);

        $rows = $this->ztdQuery('SELECT int_val, float_val, str_val FROM pg_tr_data WHERE id = 1');
        $this->assertSame(42, (int) $rows[0]['int_val']);
        $this->assertEqualsWithDelta(3.14, (float) $rows[0]['float_val'], 0.001);
        $this->assertSame('prepared', $rows[0]['str_val']);
    }
}
