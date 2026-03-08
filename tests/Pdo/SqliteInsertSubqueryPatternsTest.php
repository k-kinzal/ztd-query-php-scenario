<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests various INSERT patterns involving subqueries — real-world patterns
 * users commonly attempt with ZTD:
 * - INSERT ... SELECT with WHERE filtering
 * - INSERT ... SELECT with computed columns
 * - INSERT ... SELECT WHERE NOT EXISTS (conditional insert)
 * - INSERT from aggregated subquery
 * - INSERT with scalar subquery in VALUES
 * @spec SPEC-4.1a
 */
class SqliteInsertSubqueryPatternsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE isp_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
            'CREATE TABLE isp_archive (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
            'CREATE TABLE isp_stats (category TEXT PRIMARY KEY, product_count INTEGER, avg_price REAL)',
            'CREATE TABLE combined (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
            'CREATE TABLE source1 (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
            'CREATE TABLE source2 (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['isp_products', 'isp_archive', 'isp_stats', 'combined', 'source1', 'source2'];
    }


    /**
     * INSERT ... SELECT with WHERE clause filters shadow data correctly.
     */
    public function testInsertSelectWithWhereFilter(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        // Archive only electronics
        $affected = $this->pdo->exec("INSERT INTO isp_archive (id, name, price, category) SELECT id, name, price, category FROM isp_products WHERE category = 'electronics'");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM isp_archive");
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * INSERT ... SELECT with computed/transformed columns produces NULL values.
     * The CTE rewriter cannot transfer computed expressions (like `price * 2`)
     * to the inserted rows — the computed column becomes NULL.
     */
    public function testInsertSelectWithComputedColumnsProducesNull(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");

        // Archive with price doubled — rows are inserted but computed value is NULL
        $affected = $this->pdo->exec("INSERT INTO isp_archive (id, name, price, category) SELECT id, name, price * 2, category FROM isp_products");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT price FROM isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // Computed column is NULL — CTE rewriter limitation
        $this->assertNull($row['price']);
    }

    /**
     * INSERT ... SELECT WHERE NOT EXISTS — conditional insert pattern.
     */
    public function testInsertSelectWhereNotExists(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_archive VALUES (1, 'Already Archived', 29.99, 'electronics')");

        // Only insert products not already in archive
        $affected = $this->pdo->exec("
            INSERT INTO isp_archive (id, name, price, category)
            SELECT id, name, price, category
            FROM isp_products p
            WHERE NOT EXISTS (SELECT 1 FROM isp_archive a WHERE a.id = p.id)
        ");
        $this->assertSame(0, $affected); // Already exists

        // Add another product and retry
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");

        $affected = $this->pdo->exec("
            INSERT INTO isp_archive (id, name, price, category)
            SELECT id, name, price, category
            FROM isp_products p
            WHERE NOT EXISTS (SELECT 1 FROM isp_archive a WHERE a.id = p.id)
        ");
        $this->assertSame(1, $affected);
    }

    /**
     * INSERT ... SELECT with GROUP BY + aggregation produces NULL for aggregate columns.
     * The CTE rewriter inserts correct row count but aggregate values (COUNT, AVG) become NULL.
     */
    public function testInsertSelectWithGroupByAggregationProducesNull(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        // Compute category stats — rows are inserted but aggregate values are NULL
        $affected = $this->pdo->exec("
            INSERT INTO isp_stats (category, product_count, avg_price)
            SELECT category, COUNT(*), AVG(price)
            FROM isp_products
            GROUP BY category
        ");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT * FROM isp_stats ORDER BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        // Aggregate columns are NULL — CTE rewriter limitation
        $this->assertNull($rows[0]['product_count']);
        $this->assertNull($rows[0]['avg_price']);
    }

    /**
     * INSERT ... SELECT with UNION combines data from multiple shadow tables.
     */
    public function testInsertSelectWithUnion(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Product A', 10.00, 'cat1')");
        $this->pdo->exec("INSERT INTO isp_archive VALUES (2, 'Archive B', 20.00, 'cat2')");

        // Create a combined table
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE combined (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');
        $raw->exec('CREATE TABLE source1 (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');
        $raw->exec('CREATE TABLE source2 (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)');

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO source1 VALUES (1, 'From S1', 10.00, 'x')");
        $pdo->exec("INSERT INTO source2 VALUES (2, 'From S2', 20.00, 'y')");

        $affected = $pdo->exec("
            INSERT INTO combined (id, name, price, category)
            SELECT id, name, price, category FROM source1
            UNION ALL
            SELECT id, name, price, category FROM source2
        ");
        $this->assertSame(2, $affected);

        $stmt = $pdo->query("SELECT * FROM combined ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('From S1', $rows[0]['name']);
        $this->assertSame('From S2', $rows[1]['name']);
    }

    /**
     * INSERT ... SELECT after mutations — the SELECT reads post-mutation shadow state.
     */
    public function testInsertSelectReflectsMutations(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");

        // Mutate before archiving
        $this->pdo->exec("UPDATE isp_products SET price = 99.99 WHERE id = 1");
        $this->pdo->exec("DELETE FROM isp_products WHERE id = 2");

        // Archive current state
        $affected = $this->pdo->exec("INSERT INTO isp_archive (id, name, price, category) SELECT id, name, price, category FROM isp_products");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT price FROM isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    /**
     * Chained INSERT ... SELECT across 3 tables.
     * Step 1 (simple column copy) works; step 2 (with GROUP BY) produces NULLs.
     */
    public function testChainedInsertSelectAcrossThreeTables(): void
    {
        // products -> archive -> stats
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        // Step 1: archive all products (direct column copy works)
        $affected = $this->pdo->exec("INSERT INTO isp_archive (id, name, price, category) SELECT id, name, price, category FROM isp_products");
        $this->assertSame(3, $affected);

        // Verify step 1
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM isp_archive");
        $this->assertSame(3, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        // Step 2: compute stats from archive (GROUP BY — aggregate values become NULL)
        $affected = $this->pdo->exec("
            INSERT INTO isp_stats (category, product_count, avg_price)
            SELECT category, COUNT(*), AVG(price) FROM isp_archive GROUP BY category
        ");
        $this->assertSame(2, $affected);

        // Rows inserted but aggregate columns are NULL
        $stmt = $this->pdo->query("SELECT * FROM isp_stats ORDER BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertNull($rows[0]['product_count']); // NULL due to GROUP BY aggregate
    }

    /**
     * Workaround for INSERT...SELECT with computed/aggregated columns:
     * SELECT first, then INSERT manually.
     */
    public function testWorkaroundManualSelectThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        // Workaround: SELECT aggregated data, then INSERT manually
        $stmt = $this->pdo->query("SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_p FROM isp_products GROUP BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($rows as $row) {
            $ins = $this->pdo->prepare("INSERT INTO isp_stats (category, product_count, avg_price) VALUES (?, ?, ?)");
            $ins->execute([$row['category'], $row['cnt'], $row['avg_p']]);
        }

        $stmt = $this->pdo->query("SELECT * FROM isp_stats ORDER BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['product_count']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['avg_price'], 0.01);
    }
}
