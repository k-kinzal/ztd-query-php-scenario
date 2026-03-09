<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests an order-to-delivery tracking lifecycle: orders, shipments, and tracking events.
 * SQL patterns exercised: multi-table JOIN, date arithmetic, COUNT/SUM CASE cross-tab,
 * correlated subquery for latest event (MySQLi).
 * @spec SPEC-10.2.132
 */
class ShippingTrackerTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_st_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(100),
                order_date TEXT,
                total_amount DECIMAL(10,2)
            )',
            'CREATE TABLE mi_st_shipments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                carrier VARCHAR(50),
                tracking_number TEXT,
                shipped_date TEXT,
                estimated_delivery TEXT,
                actual_delivery TEXT
            )',
            'CREATE TABLE mi_st_tracking_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                shipment_id INT,
                event_date TEXT,
                event_type TEXT,
                location TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_st_tracking_events', 'mi_st_shipments', 'mi_st_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Orders
        $this->mysqli->query("INSERT INTO mi_st_orders VALUES (1, 'Alice', '2025-09-01', 150.00)");
        $this->mysqli->query("INSERT INTO mi_st_orders VALUES (2, 'Bob', '2025-09-05', 89.99)");
        $this->mysqli->query("INSERT INTO mi_st_orders VALUES (3, 'Carol', '2025-09-10', 320.00)");
        $this->mysqli->query("INSERT INTO mi_st_orders VALUES (4, 'Dave', '2025-09-12', 45.00)");

        // Shipments
        $this->mysqli->query("INSERT INTO mi_st_shipments VALUES (1, 1, 'FedEx', 'FX100001', '2025-09-02', '2025-09-05', '2025-09-04')");
        $this->mysqli->query("INSERT INTO mi_st_shipments VALUES (2, 2, 'UPS', 'UP200002', '2025-09-06', '2025-09-10', '2025-09-09')");
        $this->mysqli->query("INSERT INTO mi_st_shipments VALUES (3, 3, 'DHL', 'DH300003', '2025-09-11', '2025-09-15', NULL)");
        $this->mysqli->query("INSERT INTO mi_st_shipments VALUES (4, 4, 'FedEx', 'FX400004', '2025-09-13', '2025-09-16', NULL)");

        // Tracking events — Shipment 1 (FedEx, delivered)
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (1, 1, '2025-09-02', 'picked_up', 'warehouse')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (2, 1, '2025-09-03', 'in_transit', 'hub')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (3, 1, '2025-09-04', 'out_for_delivery', 'local')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (4, 1, '2025-09-04', 'delivered', 'doorstep')");

        // Tracking events — Shipment 2 (UPS, delivered)
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (5, 2, '2025-09-06', 'picked_up', 'warehouse')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (6, 2, '2025-09-07', 'in_transit', 'hub')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (7, 2, '2025-09-09', 'delivered', 'mailroom')");

        // Tracking events — Shipment 3 (DHL, in transit)
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (8, 3, '2025-09-11', 'picked_up', 'warehouse')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (9, 3, '2025-09-12', 'in_transit', 'hub')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (10, 3, '2025-09-13', 'in_transit', 'regional')");

        // Tracking events — Shipment 4 (FedEx, exception)
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (11, 4, '2025-09-13', 'picked_up', 'warehouse')");
        $this->mysqli->query("INSERT INTO mi_st_tracking_events VALUES (12, 4, '2025-09-14', 'exception', 'customs')");
    }

    /**
     * JOIN orders + shipments to show summary with delivery status.
     */
    public function testOrderShipmentSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id AS order_id, o.customer_name, s.carrier, s.shipped_date, s.actual_delivery
             FROM mi_st_orders o
             JOIN mi_st_shipments s ON s.order_id = o.id
             ORDER BY o.id"
        );

        $this->assertCount(4, $rows);

        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertSame('FedEx', $rows[0]['carrier']);
        $this->assertSame('2025-09-02', $rows[0]['shipped_date']);
        $this->assertSame('2025-09-04', $rows[0]['actual_delivery']);

        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertSame('Bob', $rows[1]['customer_name']);
        $this->assertSame('UPS', $rows[1]['carrier']);
        $this->assertSame('2025-09-06', $rows[1]['shipped_date']);
        $this->assertSame('2025-09-09', $rows[1]['actual_delivery']);

        $this->assertEquals(3, (int) $rows[2]['order_id']);
        $this->assertSame('Carol', $rows[2]['customer_name']);
        $this->assertSame('DHL', $rows[2]['carrier']);
        $this->assertSame('2025-09-11', $rows[2]['shipped_date']);
        $this->assertNull($rows[2]['actual_delivery']);

        $this->assertEquals(4, (int) $rows[3]['order_id']);
        $this->assertSame('Dave', $rows[3]['customer_name']);
        $this->assertSame('FedEx', $rows[3]['carrier']);
        $this->assertSame('2025-09-13', $rows[3]['shipped_date']);
        $this->assertNull($rows[3]['actual_delivery']);
    }

    /**
     * Correlated subquery: get latest tracking event per shipment.
     */
    public function testLatestTrackingEventPerShipment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT te.shipment_id, te.event_type, te.event_date, te.location
             FROM mi_st_tracking_events te
             WHERE te.event_date = (
                 SELECT MAX(te2.event_date)
                 FROM mi_st_tracking_events te2
                 WHERE te2.shipment_id = te.shipment_id
             )
             AND te.id = (
                 SELECT MAX(te3.id)
                 FROM mi_st_tracking_events te3
                 WHERE te3.shipment_id = te.shipment_id
                   AND te3.event_date = te.event_date
             )
             ORDER BY te.shipment_id"
        );

        $this->assertCount(4, $rows);

        $this->assertEquals(1, (int) $rows[0]['shipment_id']);
        $this->assertSame('delivered', $rows[0]['event_type']);

        $this->assertEquals(2, (int) $rows[1]['shipment_id']);
        $this->assertSame('delivered', $rows[1]['event_type']);

        $this->assertEquals(3, (int) $rows[2]['shipment_id']);
        $this->assertSame('in_transit', $rows[2]['event_type']);

        $this->assertEquals(4, (int) $rows[3]['shipment_id']);
        $this->assertSame('exception', $rows[3]['event_type']);
    }

    /**
     * Delivery rate by carrier using COUNT/SUM CASE cross-tab.
     */
    public function testDeliveryRateByCarrier(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.carrier,
                    COUNT(*) AS total_shipments,
                    SUM(CASE WHEN s.actual_delivery IS NOT NULL THEN 1 ELSE 0 END) AS delivered_count,
                    ROUND(SUM(CASE WHEN s.actual_delivery IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS delivery_rate_pct
             FROM mi_st_shipments s
             GROUP BY s.carrier
             ORDER BY s.carrier"
        );

        $this->assertCount(3, $rows);

        // DHL: 0 of 1 = 0%
        $this->assertSame('DHL', $rows[0]['carrier']);
        $this->assertEquals(1, (int) $rows[0]['total_shipments']);
        $this->assertEquals(0, (int) $rows[0]['delivered_count']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['delivery_rate_pct'], 1);

        // FedEx: 1 of 2 = 50%
        $this->assertSame('FedEx', $rows[1]['carrier']);
        $this->assertEquals(2, (int) $rows[1]['total_shipments']);
        $this->assertEquals(1, (int) $rows[1]['delivered_count']);
        $this->assertEqualsWithDelta(50.0, (float) $rows[1]['delivery_rate_pct'], 1);

        // UPS: 1 of 1 = 100%
        $this->assertSame('UPS', $rows[2]['carrier']);
        $this->assertEquals(1, (int) $rows[2]['total_shipments']);
        $this->assertEquals(1, (int) $rows[2]['delivered_count']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[2]['delivery_rate_pct'], 1);
    }

    /**
     * On-time delivery rate: compare actual_delivery <= estimated_delivery for delivered shipments.
     */
    public function testOnTimeDeliveryRate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS delivered_total,
                    SUM(CASE WHEN s.actual_delivery <= s.estimated_delivery THEN 1 ELSE 0 END) AS on_time_count,
                    ROUND(SUM(CASE WHEN s.actual_delivery <= s.estimated_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS on_time_pct
             FROM mi_st_shipments s
             WHERE s.actual_delivery IS NOT NULL"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['delivered_total']);
        $this->assertEquals(2, (int) $rows[0]['on_time_count']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['on_time_pct'], 1);
    }

    /**
     * Tracking event count by type.
     */
    public function testTrackingEventCountByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT event_type, COUNT(*) AS cnt
             FROM mi_st_tracking_events
             GROUP BY event_type
             ORDER BY event_type"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('delivered', $rows[0]['event_type']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $this->assertSame('exception', $rows[1]['event_type']);
        $this->assertEquals(1, (int) $rows[1]['cnt']);

        $this->assertSame('in_transit', $rows[2]['event_type']);
        $this->assertEquals(4, (int) $rows[2]['cnt']);

        $this->assertSame('out_for_delivery', $rows[3]['event_type']);
        $this->assertEquals(1, (int) $rows[3]['cnt']);

        $this->assertSame('picked_up', $rows[4]['event_type']);
        $this->assertEquals(4, (int) $rows[4]['cnt']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_st_orders VALUES (5, 'Eve', '2025-09-15', 200.00)");

        // ZTD sees the new order
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_st_orders");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_st_orders');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
