<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests column type edge cases: TIME, BLOB, BOOLEAN, mixed types in same query,
 * type coercion in CASE, arithmetic with mixed types.
 * @spec pending
 */
class SqliteColumnTypeEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, event_time TEXT, event_date TEXT, is_active INTEGER, payload BLOB)',
            'CREATE TABLE metrics (id INTEGER PRIMARY KEY, label TEXT, int_val INTEGER, float_val REAL, text_val TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['events', 'metrics'];
    }


    public function testTimeValuesInShadowStore(): void
    {
        $this->pdo->exec("INSERT INTO events VALUES (1, 'Morning standup', '09:30:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO events VALUES (2, 'Lunch break', '12:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO events VALUES (3, 'Evening review', '17:30:00', '2024-01-15', 0, NULL)");

        $stmt = $this->pdo->query("SELECT name, event_time FROM events WHERE event_time > '12:00:00' ORDER BY event_time");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Evening review', $rows[0]['name']);
        $this->assertSame('17:30:00', $rows[0]['event_time']);
    }

    public function testTimeComparisonAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO events VALUES (1, 'Meeting', '09:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("UPDATE events SET event_time = '14:00:00' WHERE id = 1");

        $stmt = $this->pdo->query("SELECT event_time FROM events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('14:00:00', $row['event_time']);
    }

    public function testTimeBetween(): void
    {
        $this->pdo->exec("INSERT INTO events VALUES (1, 'Early', '08:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO events VALUES (2, 'Mid', '12:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO events VALUES (3, 'Late', '18:00:00', '2024-01-15', 1, NULL)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM events WHERE event_time BETWEEN '09:00:00' AND '17:00:00'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']); // Only "Mid"
    }

    /**
     * BLOB data with binary bytes causes CTE rewriter to produce invalid SQL.
     * The rewriter embeds values as string literals, and binary data breaks the SQL syntax.
     */
    public function testBlobInsertWithBinaryDataBreaksCteRewriter(): void
    {
        $binaryData = "\x00\x01\x02\xFF\xFE";
        $stmt = $this->pdo->prepare("INSERT INTO events (id, name, event_time, event_date, is_active, payload) VALUES (?, ?, ?, ?, ?, ?)");
        $stmt->execute([1, 'Binary event', '10:00:00', '2024-01-15', 1, $binaryData]);

        $this->expectException(\Throwable::class);
        $this->pdo->query("SELECT payload FROM events WHERE id = 1");
    }

    public function testBlobInsertWithTextDataWorks(): void
    {
        $textPayload = 'hello world';
        $stmt = $this->pdo->prepare("INSERT INTO events (id, name, event_time, event_date, is_active, payload) VALUES (?, ?, ?, ?, ?, ?)");
        $stmt->execute([1, 'Text event', '10:00:00', '2024-01-15', 1, $textPayload]);

        $select = $this->pdo->query("SELECT payload FROM events WHERE id = 1");
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame($textPayload, $row['payload']);
    }

    public function testBooleanAsInteger(): void
    {
        $this->pdo->exec("INSERT INTO events VALUES (1, 'Active', '10:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO events VALUES (2, 'Inactive', '10:00:00', '2024-01-15', 0, NULL)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM events WHERE is_active = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);

        // Toggle boolean
        $this->pdo->exec("UPDATE events SET is_active = 0 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM events WHERE is_active = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testMixedTypeArithmetic(): void
    {
        $this->pdo->exec("INSERT INTO metrics VALUES (1, 'test', 10, 3.14, '42')");

        $stmt = $this->pdo->query("SELECT int_val + float_val AS sum1, int_val * 2 AS doubled, CAST(text_val AS INTEGER) + int_val AS sum2 FROM metrics WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(13.14, (float) $row['sum1'], 0.01);
        $this->assertSame(20, (int) $row['doubled']);
        $this->assertSame(52, (int) $row['sum2']);
    }

    public function testCaseWithMixedReturnTypes(): void
    {
        $this->pdo->exec("INSERT INTO metrics VALUES (1, 'high', 100, 9.99, 'premium')");
        $this->pdo->exec("INSERT INTO metrics VALUES (2, 'low', 5, 0.50, 'basic')");

        $stmt = $this->pdo->query("
            SELECT label,
                   CASE
                       WHEN int_val > 50 THEN 'large'
                       WHEN int_val > 10 THEN 'medium'
                       ELSE 'small'
                   END AS size_category,
                   CASE
                       WHEN float_val > 5 THEN float_val * 2
                       ELSE float_val
                   END AS adjusted_val
            FROM metrics ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('large', $rows[0]['size_category']);
        $this->assertEqualsWithDelta(19.98, (float) $rows[0]['adjusted_val'], 0.01);
        $this->assertSame('small', $rows[1]['size_category']);
        $this->assertEqualsWithDelta(0.50, (float) $rows[1]['adjusted_val'], 0.01);
    }

    public function testAggregateOnMixedTypes(): void
    {
        $this->pdo->exec("INSERT INTO metrics VALUES (1, 'a', 10, 1.5, 'x')");
        $this->pdo->exec("INSERT INTO metrics VALUES (2, 'b', 20, 2.5, 'y')");
        $this->pdo->exec("INSERT INTO metrics VALUES (3, 'c', 30, 3.5, 'z')");

        $stmt = $this->pdo->query("SELECT SUM(int_val) AS int_sum, AVG(float_val) AS float_avg, GROUP_CONCAT(text_val, ',') AS texts FROM metrics");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(60, (int) $row['int_sum']);
        $this->assertEqualsWithDelta(2.5, (float) $row['float_avg'], 0.01);
        $this->assertNotEmpty($row['texts']);
    }

    public function testUpdateWithArithmeticExpression(): void
    {
        $this->pdo->exec("INSERT INTO metrics VALUES (1, 'price', 100, 19.99, 'product')");

        // Increase by 10%
        $this->pdo->exec("UPDATE metrics SET float_val = float_val * 1.1 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT float_val FROM metrics WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(21.989, (float) $row['float_val'], 0.01);
    }
}
