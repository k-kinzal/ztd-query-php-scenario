<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a weekly meal planning scenario: CROSS JOIN for day x meal slot generation,
 * LEFT JOIN for unassigned slot detection, SUM aggregate for shopping list quantities,
 * GROUP BY with HAVING for dietary filters, and prepared statement for day lookup (PostgreSQL PDO).
 * @spec SPEC-10.2.146
 */
class PostgresMealPlanningTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mp_meals (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                category VARCHAR(50),
                dietary_tag VARCHAR(50)
            )',
            'CREATE TABLE pg_mp_meal_ingredients (
                id SERIAL PRIMARY KEY,
                meal_id INTEGER,
                ingredient_name VARCHAR(100),
                quantity NUMERIC(10,2),
                unit VARCHAR(50)
            )',
            'CREATE TABLE pg_mp_weekly_plan (
                id SERIAL PRIMARY KEY,
                day_of_week VARCHAR(10),
                meal_slot VARCHAR(20),
                meal_id INTEGER
            )',
            'CREATE TABLE pg_mp_days (
                id SERIAL PRIMARY KEY,
                day_name VARCHAR(10)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mp_days', 'pg_mp_meal_ingredients', 'pg_mp_meals', 'pg_mp_weekly_plan'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Meals
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (1, 'Oatmeal', 'breakfast', 'vegan')");
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (2, 'Eggs Benedict', 'breakfast', 'none')");
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (3, 'Caesar Salad', 'lunch', 'none')");
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (4, 'Veggie Wrap', 'lunch', 'vegetarian')");
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (5, 'Grilled Salmon', 'dinner', 'none')");
        $this->pdo->exec("INSERT INTO pg_mp_meals VALUES (6, 'Pasta Primavera', 'dinner', 'vegetarian')");

        // Meal ingredients
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (1, 1, 'oats', 1.00, 'cup')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (2, 1, 'almond milk', 1.50, 'cup')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (3, 1, 'honey', 1.00, 'tbsp')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (4, 2, 'eggs', 2.00, 'each')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (5, 2, 'english muffin', 1.00, 'each')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (6, 2, 'hollandaise', 2.00, 'tbsp')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (7, 3, 'romaine', 2.00, 'cup')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (8, 3, 'croutons', 0.50, 'cup')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (9, 3, 'parmesan', 1.00, 'tbsp')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (10, 4, 'tortilla', 1.00, 'each')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (11, 4, 'hummus', 2.00, 'tbsp')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (12, 4, 'mixed greens', 1.00, 'cup')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (13, 5, 'salmon', 6.00, 'oz')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (14, 5, 'lemon', 1.00, 'each')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (15, 5, 'asparagus', 4.00, 'spear')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (16, 6, 'pasta', 8.00, 'oz')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (17, 6, 'bell pepper', 1.00, 'each')");
        $this->pdo->exec("INSERT INTO pg_mp_meal_ingredients VALUES (18, 6, 'olive oil', 1.00, 'tbsp')");

        // Days reference
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (1, 'Mon')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (2, 'Tue')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (3, 'Wed')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (4, 'Thu')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (5, 'Fri')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (6, 'Sat')");
        $this->pdo->exec("INSERT INTO pg_mp_days VALUES (7, 'Sun')");

        // Weekly plan (partially filled)
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (1, 'Mon', 'breakfast', 1)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (2, 'Mon', 'lunch', 4)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (3, 'Mon', 'dinner', 5)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (4, 'Tue', 'breakfast', 2)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (5, 'Tue', 'lunch', 3)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (6, 'Tue', 'dinner', 6)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (7, 'Wed', 'breakfast', 1)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (8, 'Wed', 'dinner', 5)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (9, 'Thu', 'lunch', 4)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (10, 'Thu', 'dinner', 6)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (11, 'Fri', 'breakfast', 2)");
        $this->pdo->exec("INSERT INTO pg_mp_weekly_plan VALUES (12, 'Fri', 'lunch', 3)");
    }

    /**
     * GROUP BY category with COUNT to see meal distribution.
     * breakfast=2, dinner=2, lunch=2.
     */
    public function testMealsByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, COUNT(*) AS cnt
             FROM pg_mp_meals
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('breakfast', $rows[0]['category']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $this->assertSame('dinner', $rows[1]['category']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);

        $this->assertSame('lunch', $rows[2]['category']);
        $this->assertEquals(2, (int) $rows[2]['cnt']);
    }

    /**
     * JOIN weekly_plan with meals to show all assigned slots.
     * 12 assigned slots ordered by day and slot.
     */
    public function testWeeklyPlanOverview(): void
    {
        $rows = $this->ztdQuery(
            "SELECT wp.day_of_week, wp.meal_slot, m.name
             FROM pg_mp_weekly_plan wp
             JOIN pg_mp_meals m ON m.id = wp.meal_id
             JOIN pg_mp_days d ON d.day_name = wp.day_of_week
             ORDER BY d.id,
                 CASE wp.meal_slot
                     WHEN 'breakfast' THEN 1
                     WHEN 'lunch' THEN 2
                     WHEN 'dinner' THEN 3
                 END"
        );

        $this->assertCount(12, $rows);

        // Mon
        $this->assertSame('Mon', $rows[0]['day_of_week']);
        $this->assertSame('breakfast', $rows[0]['meal_slot']);
        $this->assertSame('Oatmeal', $rows[0]['name']);

        $this->assertSame('Mon', $rows[1]['day_of_week']);
        $this->assertSame('lunch', $rows[1]['meal_slot']);
        $this->assertSame('Veggie Wrap', $rows[1]['name']);

        $this->assertSame('Mon', $rows[2]['day_of_week']);
        $this->assertSame('dinner', $rows[2]['meal_slot']);
        $this->assertSame('Grilled Salmon', $rows[2]['name']);

        // Tue
        $this->assertSame('Tue', $rows[3]['day_of_week']);
        $this->assertSame('breakfast', $rows[3]['meal_slot']);
        $this->assertSame('Eggs Benedict', $rows[3]['name']);
    }

    /**
     * CROSS JOIN days x slots, LEFT JOIN plan to find unassigned slots.
     * Wed lunch, Thu breakfast, Fri dinner, plus all 6 Sat/Sun slots = 9 unassigned.
     */
    public function testUnassignedSlots(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.day_name, slots.slot
             FROM pg_mp_days d
             CROSS JOIN (
                 SELECT 'breakfast' AS slot
                 UNION ALL SELECT 'lunch'
                 UNION ALL SELECT 'dinner'
             ) AS slots
             LEFT JOIN pg_mp_weekly_plan wp
                 ON wp.day_of_week = d.day_name AND wp.meal_slot = slots.slot
             WHERE wp.id IS NULL
             ORDER BY d.id,
                 CASE slots.slot
                     WHEN 'breakfast' THEN 1
                     WHEN 'lunch' THEN 2
                     WHEN 'dinner' THEN 3
                 END"
        );

        $this->assertCount(9, $rows);

        // Wed lunch
        $this->assertSame('Wed', $rows[0]['day_name']);
        $this->assertSame('lunch', $rows[0]['slot']);

        // Thu breakfast
        $this->assertSame('Thu', $rows[1]['day_name']);
        $this->assertSame('breakfast', $rows[1]['slot']);

        // Fri dinner
        $this->assertSame('Fri', $rows[2]['day_name']);
        $this->assertSame('dinner', $rows[2]['slot']);

        // Sat: all 3 slots
        $this->assertSame('Sat', $rows[3]['day_name']);
        $this->assertSame('breakfast', $rows[3]['slot']);
        $this->assertSame('Sat', $rows[4]['day_name']);
        $this->assertSame('lunch', $rows[4]['slot']);
        $this->assertSame('Sat', $rows[5]['day_name']);
        $this->assertSame('dinner', $rows[5]['slot']);

        // Sun: all 3 slots
        $this->assertSame('Sun', $rows[6]['day_name']);
        $this->assertSame('breakfast', $rows[6]['slot']);
        $this->assertSame('Sun', $rows[7]['day_name']);
        $this->assertSame('lunch', $rows[7]['slot']);
        $this->assertSame('Sun', $rows[8]['day_name']);
        $this->assertSame('dinner', $rows[8]['slot']);
    }

    /**
     * SUM quantities across all planned meals, GROUP BY ingredient.
     * Oatmeal appears twice (Mon, Wed): oats = 2 x 1.0 = 2.0.
     * Grilled Salmon appears twice (Mon, Wed): salmon = 2 x 6.0 = 12.0.
     */
    public function testShoppingListAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT mi.ingredient_name, SUM(mi.quantity) AS total_qty, mi.unit
             FROM pg_mp_weekly_plan wp
             JOIN pg_mp_meal_ingredients mi ON mi.meal_id = wp.meal_id
             GROUP BY mi.ingredient_name, mi.unit
             ORDER BY mi.ingredient_name"
        );

        // 18 distinct ingredients across 6 meals, but only planned meals contribute
        $this->assertGreaterThan(0, count($rows));

        // Find oats: Oatmeal used Mon + Wed = 2 times, 1.0 cup each = 2.0
        $oats = array_values(array_filter($rows, fn($r) => $r['ingredient_name'] === 'oats'));
        $this->assertCount(1, $oats);
        $this->assertEqualsWithDelta(2.0, (float) $oats[0]['total_qty'], 0.01);
        $this->assertSame('cup', $oats[0]['unit']);

        // Find salmon: Grilled Salmon used Mon + Wed = 2 times, 6.0 oz each = 12.0
        $salmon = array_values(array_filter($rows, fn($r) => $r['ingredient_name'] === 'salmon'));
        $this->assertCount(1, $salmon);
        $this->assertEqualsWithDelta(12.0, (float) $salmon[0]['total_qty'], 0.01);
        $this->assertSame('oz', $salmon[0]['unit']);
    }

    /**
     * Filter meals by dietary_tag with GROUP BY and HAVING.
     * vegetarian/vegan meals: Oatmeal, Veggie Wrap, Pasta Primavera = 3 meals.
     */
    public function testVegetarianMealsOnly(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name, m.category, COUNT(mi.id) AS ingredient_count
             FROM pg_mp_meals m
             JOIN pg_mp_meal_ingredients mi ON mi.meal_id = m.id
             WHERE m.dietary_tag IN ('vegetarian', 'vegan')
             GROUP BY m.id, m.name, m.category
             HAVING COUNT(mi.id) >= 1
             ORDER BY m.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Oatmeal', $rows[0]['name']);
        $this->assertSame('breakfast', $rows[0]['category']);
        $this->assertEquals(3, (int) $rows[0]['ingredient_count']);

        $this->assertSame('Pasta Primavera', $rows[1]['name']);
        $this->assertSame('dinner', $rows[1]['category']);
        $this->assertEquals(3, (int) $rows[1]['ingredient_count']);

        $this->assertSame('Veggie Wrap', $rows[2]['name']);
        $this->assertSame('lunch', $rows[2]['category']);
        $this->assertEquals(3, (int) $rows[2]['ingredient_count']);
    }

    /**
     * Prepared statement: lookup all meals for a given day_of_week.
     * 'Tue' has 3 assigned meals: Eggs Benedict (breakfast), Caesar Salad (lunch), Pasta Primavera (dinner).
     */
    public function testPreparedMealsByDay(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT wp.meal_slot, m.name
             FROM pg_mp_weekly_plan wp
             JOIN pg_mp_meals m ON m.id = wp.meal_id
             WHERE wp.day_of_week = ?
             ORDER BY
                 CASE wp.meal_slot
                     WHEN 'breakfast' THEN 1
                     WHEN 'lunch' THEN 2
                     WHEN 'dinner' THEN 3
                 END",
            ['Tue']
        );

        $this->assertCount(3, $rows);

        $this->assertSame('breakfast', $rows[0]['meal_slot']);
        $this->assertSame('Eggs Benedict', $rows[0]['name']);

        $this->assertSame('lunch', $rows[1]['meal_slot']);
        $this->assertSame('Caesar Salad', $rows[1]['name']);

        $this->assertSame('dinner', $rows[2]['meal_slot']);
        $this->assertSame('Pasta Primavera', $rows[2]['name']);
    }

    /**
     * Physical isolation: ZTD sees all weekly_plan rows, physical table is empty.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_mp_weekly_plan");
        $this->assertEquals(12, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_mp_weekly_plan")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
