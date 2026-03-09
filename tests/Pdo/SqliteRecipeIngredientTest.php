<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a recipe ingredient management workflow through ZTD shadow store (SQLite PDO).
 * Covers JOIN for recipe ingredients, scaled quantity calculations, shopping list
 * aggregation, LEFT JOIN for substitutions, ingredient count per recipe, and physical isolation.
 * @spec SPEC-10.2.86
 */
class SqliteRecipeIngredientTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ri_recipes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                servings INTEGER
            )',
            'CREATE TABLE sl_ri_ingredients (
                id INTEGER PRIMARY KEY,
                recipe_id INTEGER,
                item_name TEXT,
                quantity REAL,
                unit TEXT
            )',
            'CREATE TABLE sl_ri_substitutions (
                ingredient_id INTEGER PRIMARY KEY,
                substitute_name TEXT,
                ratio REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ri_substitutions', 'sl_ri_ingredients', 'sl_ri_recipes'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 recipes
        $this->pdo->exec("INSERT INTO sl_ri_recipes VALUES (1, 'Pancakes', 4)");
        $this->pdo->exec("INSERT INTO sl_ri_recipes VALUES (2, 'Omelette', 2)");

        // Ingredients for Pancakes (recipe 1)
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (1, 1, 'flour', 2.00, 'cups')");
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (2, 1, 'eggs', 2.00, 'pieces')");
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (3, 1, 'milk', 1.50, 'cups')");
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (4, 1, 'sugar', 0.25, 'cups')");

        // Ingredients for Omelette (recipe 2)
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (5, 2, 'eggs', 3.00, 'pieces')");
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (6, 2, 'milk', 0.25, 'cups')");
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (7, 2, 'cheese', 0.50, 'cups')");

        // Substitutions: flour -> almond flour (1.0x), milk -> oat milk (1.0x)
        $this->pdo->exec("INSERT INTO sl_ri_substitutions VALUES (1, 'almond flour', 1.00)");
        $this->pdo->exec("INSERT INTO sl_ri_substitutions VALUES (3, 'oat milk', 1.00)");
    }

    /**
     * List all ingredients for a recipe via JOIN with prepared statement.
     */
    public function testRecipeIngredients(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.name AS recipe_name, i.item_name, i.quantity, i.unit
             FROM sl_ri_recipes r
             JOIN sl_ri_ingredients i ON i.recipe_id = r.id
             WHERE r.id = ?
             ORDER BY i.id",
            [1]
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Pancakes', $rows[0]['recipe_name']);
        $this->assertSame('flour', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(2.00, (float) $rows[0]['quantity'], 0.01);
        $this->assertSame('cups', $rows[0]['unit']);
        $this->assertSame('sugar', $rows[3]['item_name']);
    }

    /**
     * Scale quantities by a factor (double servings): quantity * 2 AS scaled_qty.
     */
    public function testScaledQuantities(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.item_name, i.quantity, i.quantity * 2 AS scaled_qty, i.unit
             FROM sl_ri_ingredients i
             WHERE i.recipe_id = 1
             ORDER BY i.id"
        );

        $this->assertCount(4, $rows);

        // flour: 2.00 -> 4.00
        $this->assertSame('flour', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(2.00, (float) $rows[0]['quantity'], 0.01);
        $this->assertEqualsWithDelta(4.00, (float) $rows[0]['scaled_qty'], 0.01);

        // eggs: 2.00 -> 4.00
        $this->assertSame('eggs', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(4.00, (float) $rows[1]['scaled_qty'], 0.01);

        // milk: 1.50 -> 3.00
        $this->assertSame('milk', $rows[2]['item_name']);
        $this->assertEqualsWithDelta(3.00, (float) $rows[2]['scaled_qty'], 0.01);

        // sugar: 0.25 -> 0.50
        $this->assertSame('sugar', $rows[3]['item_name']);
        $this->assertEqualsWithDelta(0.50, (float) $rows[3]['scaled_qty'], 0.01);
    }

    /**
     * Shopping list aggregation: SUM(quantity) GROUP BY item_name across multiple recipes.
     */
    public function testShoppingListAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.item_name, SUM(i.quantity) AS total_qty
             FROM sl_ri_ingredients i
             WHERE i.recipe_id IN (1, 2)
             GROUP BY i.item_name
             ORDER BY i.item_name"
        );

        $this->assertCount(5, $rows);

        // cheese: 0.50 (only omelette)
        $this->assertSame('cheese', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(0.50, (float) $rows[0]['total_qty'], 0.01);

        // eggs: 2.00 + 3.00 = 5.00
        $this->assertSame('eggs', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(5.00, (float) $rows[1]['total_qty'], 0.01);

        // flour: 2.00 (only pancakes)
        $this->assertSame('flour', $rows[2]['item_name']);
        $this->assertEqualsWithDelta(2.00, (float) $rows[2]['total_qty'], 0.01);

        // milk: 1.50 + 0.25 = 1.75
        $this->assertSame('milk', $rows[3]['item_name']);
        $this->assertEqualsWithDelta(1.75, (float) $rows[3]['total_qty'], 0.01);

        // sugar: 0.25 (only pancakes)
        $this->assertSame('sugar', $rows[4]['item_name']);
        $this->assertEqualsWithDelta(0.25, (float) $rows[4]['total_qty'], 0.01);
    }

    /**
     * Show ingredients with available substitutions via LEFT JOIN.
     */
    public function testAvailableSubstitutions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.item_name, s.substitute_name, s.ratio
             FROM sl_ri_ingredients i
             LEFT JOIN sl_ri_substitutions s ON s.ingredient_id = i.id
             WHERE i.recipe_id = 1
             ORDER BY i.id"
        );

        $this->assertCount(4, $rows);

        // flour has a substitution
        $this->assertSame('flour', $rows[0]['item_name']);
        $this->assertSame('almond flour', $rows[0]['substitute_name']);
        $this->assertEqualsWithDelta(1.00, (float) $rows[0]['ratio'], 0.01);

        // eggs has no substitution
        $this->assertSame('eggs', $rows[1]['item_name']);
        $this->assertNull($rows[1]['substitute_name']);

        // milk has a substitution
        $this->assertSame('milk', $rows[2]['item_name']);
        $this->assertSame('oat milk', $rows[2]['substitute_name']);

        // sugar has no substitution
        $this->assertSame('sugar', $rows[3]['item_name']);
        $this->assertNull($rows[3]['substitute_name']);
    }

    /**
     * Add an ingredient to a recipe and verify ingredient count changes.
     */
    public function testAddIngredient(): void
    {
        // Verify omelette has 3 ingredients before
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_ri_ingredients WHERE recipe_id = 2"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Add salt to omelette
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (8, 2, 'salt', 0.50, 'tsp')");

        // Verify omelette now has 4 ingredients
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_ri_ingredients WHERE recipe_id = 2"
        );
        $this->assertEquals(4, (int) $rows[0]['cnt']);
    }

    /**
     * Ingredient count per recipe via LEFT JOIN, ordered by count.
     */
    public function testIngredientCountPerRecipe(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(i.id) AS ingredient_count
             FROM sl_ri_recipes r
             LEFT JOIN sl_ri_ingredients i ON i.recipe_id = r.id
             GROUP BY r.id, r.name
             ORDER BY ingredient_count DESC"
        );

        $this->assertCount(2, $rows);

        // Pancakes: 4 ingredients
        $this->assertSame('Pancakes', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['ingredient_count']);

        // Omelette: 3 ingredients
        $this->assertSame('Omelette', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['ingredient_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_ri_ingredients VALUES (8, 2, 'pepper', 0.25, 'tsp')");
        $this->pdo->exec("UPDATE sl_ri_recipes SET servings = 8 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ri_ingredients");
        $this->assertSame(8, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT servings FROM sl_ri_recipes WHERE id = 1");
        $this->assertEquals(8, (int) $rows[0]['servings']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ri_ingredients")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
