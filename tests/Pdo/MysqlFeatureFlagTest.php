<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a feature flag / A-B testing system through ZTD shadow store (MySQL PDO).
 * Covers conditional rollout evaluation, percentage-based segmentation,
 * experiment result aggregation, and physical isolation.
 * @spec SPEC-10.2.107
 */
class MysqlFeatureFlagTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ff_features (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                enabled INT,
                rollout_pct INT,
                description VARCHAR(1000)
            )',
            'CREATE TABLE mp_ff_user_segments (
                id INT PRIMARY KEY,
                user_id INT,
                segment VARCHAR(255)
            )',
            'CREATE TABLE mp_ff_experiment_results (
                id INT PRIMARY KEY,
                feature_id INT,
                user_id INT,
                variant VARCHAR(255),
                converted INT,
                revenue DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ff_experiment_results', 'mp_ff_user_segments', 'mp_ff_features'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Features
        $this->pdo->exec("INSERT INTO mp_ff_features VALUES (1, 'new_checkout', 1, 50, 'Redesigned checkout flow')");
        $this->pdo->exec("INSERT INTO mp_ff_features VALUES (2, 'dark_mode', 1, 100, 'Dark mode theme')");
        $this->pdo->exec("INSERT INTO mp_ff_features VALUES (3, 'ai_search', 0, 0, 'AI-powered search')");

        // User segments
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (1, 1, 'beta')");
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (2, 2, 'beta')");
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (3, 3, 'general')");
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (4, 4, 'general')");
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (5, 5, 'general')");
        $this->pdo->exec("INSERT INTO mp_ff_user_segments VALUES (6, 6, 'general')");

        // Experiment results for new_checkout A/B test
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (1,  1, 1, 'control', 0, 0)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (2,  1, 2, 'variant', 1, 49.99)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (3,  1, 3, 'control', 1, 29.99)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (4,  1, 4, 'variant', 1, 79.99)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (5,  1, 5, 'control', 0, 0)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (6,  1, 6, 'variant', 1, 59.99)");
    }

    /**
     * List all enabled features with their rollout percentage.
     */
    public function testListEnabledFeatures(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, rollout_pct, description
             FROM mp_ff_features
             WHERE enabled = 1
             ORDER BY name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('dark_mode', $rows[0]['name']);
        $this->assertEquals(100, (int) $rows[0]['rollout_pct']);
        $this->assertSame('new_checkout', $rows[1]['name']);
        $this->assertEquals(50, (int) $rows[1]['rollout_pct']);
    }

    /**
     * A/B test results: conversion rate and average revenue per variant.
     */
    public function testExperimentResults(): void
    {
        $rows = $this->ztdQuery(
            "SELECT er.variant,
                    COUNT(*) AS participants,
                    SUM(er.converted) AS conversions,
                    ROUND(SUM(er.converted) * 100.0 / COUNT(*), 1) AS conversion_rate,
                    ROUND(AVG(CASE WHEN er.converted = 1 THEN er.revenue END), 2) AS avg_revenue
             FROM mp_ff_experiment_results er
             WHERE er.feature_id = 1
             GROUP BY er.variant
             ORDER BY er.variant"
        );

        $this->assertCount(2, $rows);

        // Control: 3 participants, 1 conversion (33.3%)
        $this->assertSame('control', $rows[0]['variant']);
        $this->assertEquals(3, (int) $rows[0]['participants']);
        $this->assertEquals(1, (int) $rows[0]['conversions']);
        $this->assertEqualsWithDelta(33.3, (float) $rows[0]['conversion_rate'], 0.1);

        // Variant: 3 participants, 3 conversions (100%)
        $this->assertSame('variant', $rows[1]['variant']);
        $this->assertEquals(3, (int) $rows[1]['participants']);
        $this->assertEquals(3, (int) $rows[1]['conversions']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[1]['conversion_rate'], 0.1);
    }

    /**
     * Experiment results by user segment.
     */
    public function testResultsBySegment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT us.segment, er.variant,
                    COUNT(*) AS participants,
                    SUM(er.converted) AS conversions
             FROM mp_ff_experiment_results er
             JOIN mp_ff_user_segments us ON us.user_id = er.user_id
             WHERE er.feature_id = 1
             GROUP BY us.segment, er.variant
             ORDER BY us.segment, er.variant"
        );

        $this->assertGreaterThanOrEqual(2, count($rows));

        // Beta users (1, 2): one control, one variant
        $beta = array_values(array_filter($rows, fn($r) => $r['segment'] === 'beta'));
        $this->assertCount(2, $beta);
    }

    /**
     * Toggle a feature and verify.
     */
    public function testToggleFeature(): void
    {
        // Disable new_checkout
        $this->pdo->exec("UPDATE mp_ff_features SET enabled = 0, rollout_pct = 0 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT enabled, rollout_pct FROM mp_ff_features WHERE id = 1");
        $this->assertEquals(0, (int) $rows[0]['enabled']);

        // Enable ai_search
        $this->pdo->exec("UPDATE mp_ff_features SET enabled = 1, rollout_pct = 10 WHERE id = 3");

        $rows = $this->ztdQuery("SELECT name, enabled, rollout_pct FROM mp_ff_features WHERE enabled = 1 ORDER BY name");
        $this->assertCount(2, $rows);
        $this->assertSame('ai_search', $rows[0]['name']);
        $this->assertSame('dark_mode', $rows[1]['name']);
    }

    /**
     * Add experiment results and verify aggregation updates.
     */
    public function testAddExperimentResults(): void
    {
        // Add more participants
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (7,  1, 7, 'control', 1, 39.99)");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (8,  1, 8, 'variant', 0, 0)");

        $rows = $this->ztdQuery(
            "SELECT variant,
                    COUNT(*) AS participants,
                    SUM(converted) AS conversions
             FROM mp_ff_experiment_results
             WHERE feature_id = 1
             GROUP BY variant
             ORDER BY variant"
        );

        // Control: 4 participants, 2 conversions
        $this->assertEquals(4, (int) $rows[0]['participants']);
        $this->assertEquals(2, (int) $rows[0]['conversions']);

        // Variant: 4 participants, 3 conversions
        $this->assertEquals(4, (int) $rows[1]['participants']);
        $this->assertEquals(3, (int) $rows[1]['conversions']);
    }

    /**
     * Feature flag evaluation: join features with user segment for eligibility.
     */
    public function testFeatureEligibility(): void
    {
        $rows = $this->ztdQuery(
            "SELECT f.name AS feature_name,
                    us.user_id,
                    us.segment,
                    CASE
                        WHEN f.enabled = 0 THEN 'disabled'
                        WHEN f.rollout_pct = 100 THEN 'enabled'
                        WHEN us.segment = 'beta' THEN 'enabled'
                        ELSE 'disabled'
                    END AS status
             FROM mp_ff_features f
             CROSS JOIN mp_ff_user_segments us
             ORDER BY f.name, us.user_id"
        );

        // 3 features x 6 users = 18 rows
        $this->assertCount(18, $rows);

        // dark_mode (100% rollout) should be enabled for all
        $darkMode = array_filter($rows, fn($r) => $r['feature_name'] === 'dark_mode');
        foreach ($darkMode as $r) {
            $this->assertSame('enabled', $r['status']);
        }

        // ai_search (disabled) should be disabled for all
        $aiSearch = array_filter($rows, fn($r) => $r['feature_name'] === 'ai_search');
        foreach ($aiSearch as $r) {
            $this->assertSame('disabled', $r['status']);
        }

        // new_checkout (50%) should be enabled for beta users, disabled for general
        $newCheckout = array_filter($rows, fn($r) => $r['feature_name'] === 'new_checkout');
        foreach ($newCheckout as $r) {
            if ($r['segment'] === 'beta') {
                $this->assertSame('enabled', $r['status']);
            } else {
                $this->assertSame('disabled', $r['status']);
            }
        }
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE mp_ff_features SET enabled = 0 WHERE id = 1");
        $this->pdo->exec("INSERT INTO mp_ff_experiment_results VALUES (9, 2, 1, 'control', 1, 10.00)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT enabled FROM mp_ff_features WHERE id = 1");
        $this->assertEquals(0, (int) $rows[0]['enabled']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ff_experiment_results");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ff_experiment_results")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
