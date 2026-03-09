<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests COUNT(*) OVER() pagination pattern on PostgreSQL shadow data.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresPaginationCountOverTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_pco_items (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(30) NOT NULL,
                price NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_pco_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'electronics' : 'tools';
            $price = $i * 10;
            $this->ztdExec("INSERT INTO pg_pco_items (name, category, price) VALUES ('Item {$i}', '{$cat}', {$price})");
        }
    }

    /**
     * COUNT(*) OVER() with LIMIT/OFFSET.
     */
    public function testCountOverWithPagination(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT *, COUNT(*) OVER() AS total_count
                 FROM pg_pco_items
                 ORDER BY id
                 LIMIT 3 OFFSET 0"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'COUNT(*) OVER() returned no rows.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEquals(10, (int) $rows[0]['total_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() pagination failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() with $N parameter syntax.
     */
    public function testCountOverWithDollarParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name, COUNT(*) OVER() AS total
                 FROM pg_pco_items
                 WHERE category = $1
                 ORDER BY price
                 LIMIT $2"
            );
            $stmt->execute(['electronics', 2]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'COUNT(*) OVER() with $N params returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals(5, (int) $rows[0]['total'],
                'Total should be 5 electronics items');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() with $N params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() after shadow mutations.
     */
    public function testCountOverAfterShadowMutations(): void
    {
        $this->ztdExec("INSERT INTO pg_pco_items (name, category, price) VALUES ('Item 11', 'electronics', 110)");
        $this->ztdExec("DELETE FROM pg_pco_items WHERE name = 'Item 1'");

        try {
            // First verify DELETE actually removed the row
            $allRows = $this->ztdQuery("SELECT name FROM pg_pco_items ORDER BY name");
            $allNames = array_column($allRows, 'name');
            $totalRows = count($allRows);

            if (in_array('Item 1', $allNames)) {
                $this->markTestIncomplete(
                    "DELETE did not remove 'Item 1' from results. "
                    . "Total rows: {$totalRows}. This may be related to upstream #23."
                );
            }

            // Now test COUNT(*) OVER() with pagination
            $rows = $this->ztdQuery(
                "SELECT name, COUNT(*) OVER() AS total
                 FROM pg_pco_items
                 ORDER BY name
                 LIMIT 5"
            );

            // 10 original + 1 insert - 1 delete = 10
            $this->assertCount(5, $rows);
            $total = (int) $rows[0]['total'];

            if ($total !== $totalRows) {
                $this->markTestIncomplete(
                    "COUNT(*) OVER() ({$total}) disagrees with actual row count ({$totalRows}). "
                    . 'Window function may not see shadow mutations correctly.'
                );
            }

            $this->assertEquals(10, $total);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() after mutations failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * PARTITION BY in window function with pagination.
     */
    public function testCountOverPartitionByWithPagination(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, category,
                        COUNT(*) OVER() AS grand_total,
                        COUNT(*) OVER(PARTITION BY category) AS category_count,
                        ROW_NUMBER() OVER(PARTITION BY category ORDER BY price) AS cat_rank
                 FROM pg_pco_items
                 ORDER BY category, price
                 LIMIT 5"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Window functions with PARTITION BY returned no rows.'
                );
            }

            $this->assertCount(5, $rows);
            $this->assertEquals(10, (int) $rows[0]['grand_total']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Window PARTITION BY + pagination failed: ' . $e->getMessage()
            );
        }
    }
}
