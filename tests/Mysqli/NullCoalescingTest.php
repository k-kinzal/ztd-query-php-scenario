<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests NULL handling edge cases through ZTD shadow store (MySQLi).
 * Covers COALESCE chains, IS NULL filtering, LEFT JOIN with NULL keys,
 * NULL in aggregation, UPDATE to NULL, NULLIF-equivalent patterns,
 * and physical isolation.
 * @spec SPEC-10.2.89
 */
class NullCoalescingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_nc_users (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(50),
                preferred_contact VARCHAR(20)
            )',
            'CREATE TABLE mi_nc_orders (
                id INT PRIMARY KEY,
                user_id INT,
                shipping_address VARCHAR(255),
                billing_address VARCHAR(255),
                notes VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_nc_orders', 'mi_nc_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users with some NULL fields
        $this->mysqli->query("INSERT INTO mi_nc_users VALUES (1, 'Alice', 'alice@example.com', '555-0101', 'email')");
        $this->mysqli->query("INSERT INTO mi_nc_users VALUES (2, 'Bob', NULL, '555-0202', 'phone')");
        $this->mysqli->query("INSERT INTO mi_nc_users VALUES (3, 'Charlie', 'charlie@example.com', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_nc_users VALUES (4, 'Diana', NULL, NULL, NULL)");

        // Orders with some NULL addresses
        $this->mysqli->query("INSERT INTO mi_nc_orders VALUES (1, 1, '123 Main St', '123 Main St', 'Rush delivery')");
        $this->mysqli->query("INSERT INTO mi_nc_orders VALUES (2, 1, '456 Oak Ave', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_nc_orders VALUES (3, 2, NULL, '789 Pine Rd', 'Gift wrap')");
        $this->mysqli->query("INSERT INTO mi_nc_orders VALUES (4, 3, NULL, NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_nc_orders VALUES (5, 5, '999 Elm St', '999 Elm St', NULL)");
    }

    /**
     * COALESCE chain: fall through NULL values to the first non-NULL.
     */
    public function testCoalesceChain(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(shipping_address, billing_address, 'No address') AS address
             FROM mi_nc_orders
             ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('123 Main St', $rows[0]['address']);   // shipping present
        $this->assertSame('456 Oak Ave', $rows[1]['address']);   // shipping present
        $this->assertSame('789 Pine Rd', $rows[2]['address']);   // shipping NULL, billing present
        $this->assertSame('No address', $rows[3]['address']);    // both NULL, fallback
        $this->assertSame('999 Elm St', $rows[4]['address']);    // shipping present
    }

    /**
     * IS NULL filtering and COUNT with NULL vs non-NULL values.
     */
    public function testNullSafeComparison(): void
    {
        // Users with NULL email
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_nc_users WHERE email IS NULL ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);

        // COUNT(*) vs COUNT(email) to show NULL exclusion
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total, COUNT(email) AS with_email FROM mi_nc_users"
        );
        $this->assertEquals(4, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['with_email']);
    }

    /**
     * LEFT JOIN where right side has no match: verify NULL columns.
     */
    public function testLeftJoinWithNullKeys(): void
    {
        // Order 5 has user_id=5 which doesn't exist
        $rows = $this->ztdQuery(
            "SELECT o.id AS order_id, o.user_id, u.name
             FROM mi_nc_orders o
             LEFT JOIN mi_nc_users u ON u.id = o.user_id
             ORDER BY o.id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertSame('Charlie', $rows[3]['name']);
        $this->assertNull($rows[4]['name']); // no matching user for user_id=5
    }

    /**
     * NULL in aggregation: COUNT(*) vs COUNT(column), AVG with NULLs.
     */
    public function testNullInAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total_orders,
                    COUNT(shipping_address) AS with_shipping,
                    COUNT(billing_address) AS with_billing,
                    COUNT(notes) AS with_notes
             FROM mi_nc_orders"
        );

        $this->assertEquals(5, (int) $rows[0]['total_orders']);
        $this->assertEquals(3, (int) $rows[0]['with_shipping']);  // orders 1,2,5 have shipping
        $this->assertEquals(3, (int) $rows[0]['with_billing']);   // orders 1,3,5 have billing
        $this->assertEquals(2, (int) $rows[0]['with_notes']);     // orders 1,3 have notes
    }

    /**
     * UPDATE a column to NULL, then verify IS NULL works on the updated row.
     */
    public function testUpdateToNull(): void
    {
        // Clear Alice's phone
        $this->mysqli->query("UPDATE mi_nc_users SET phone = NULL WHERE id = 1");

        // Verify IS NULL
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_nc_users WHERE phone IS NULL ORDER BY id"
        );
        $this->assertCount(3, $rows); // Alice (updated), Charlie, Diana
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
        $this->assertSame('Diana', $rows[2]['name']);

        // Verify IS NOT NULL
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_nc_users WHERE phone IS NOT NULL ORDER BY id"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * CASE WHEN equivalent of NULLIF: return NULL when billing equals shipping, otherwise billing.
     */
    public function testNullIfFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id,
                    CASE WHEN billing_address = shipping_address THEN NULL ELSE billing_address END AS distinct_billing
             FROM mi_nc_orders
             ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertNull($rows[0]['distinct_billing']);          // order 1: billing = shipping
        $this->assertNull($rows[1]['distinct_billing']);          // order 2: billing is NULL -> ELSE NULL
        $this->assertSame('789 Pine Rd', $rows[2]['distinct_billing']); // order 3: shipping NULL, billing != shipping
        $this->assertNull($rows[3]['distinct_billing']);          // order 4: both NULL -> NULL = NULL is unknown, ELSE NULL
        $this->assertNull($rows[4]['distinct_billing']);          // order 5: billing = shipping
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_nc_users SET phone = NULL WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_nc_users VALUES (5, 'Eve', NULL, NULL, NULL)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_nc_users");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_nc_users');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
