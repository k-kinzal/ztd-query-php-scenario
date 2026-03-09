<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests internationalization content storage with language fallback chains (MySQLi).
 *
 * Exercises LEFT JOIN for language fallback (requested → default), COALESCE to
 * pick the first non-NULL translation, GROUP BY with COUNT for translation
 * coverage stats, prepared statements with language parameters, and multi-table
 * JOINs across content_items, translations, and languages.
 * @spec SPEC-10.2.114
 */
class MultiLanguageContentTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mlc_languages (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(50),
                is_default INT
            )',
            'CREATE TABLE mi_mlc_content_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                slug VARCHAR(100),
                content_type VARCHAR(50)
            )',
            'CREATE TABLE mi_mlc_translations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                content_id INT,
                lang_code VARCHAR(10),
                title VARCHAR(255),
                body VARCHAR(1000)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mlc_translations', 'mi_mlc_content_items', 'mi_mlc_languages'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Languages
        $this->mysqli->query("INSERT INTO mi_mlc_languages VALUES ('en', 'English', 1)");
        $this->mysqli->query("INSERT INTO mi_mlc_languages VALUES ('ja', 'Japanese', 0)");
        $this->mysqli->query("INSERT INTO mi_mlc_languages VALUES ('fr', 'French', 0)");
        $this->mysqli->query("INSERT INTO mi_mlc_languages VALUES ('de', 'German', 0)");

        // Content items
        $this->mysqli->query("INSERT INTO mi_mlc_content_items VALUES (1, 'welcome-page', 'page')");
        $this->mysqli->query("INSERT INTO mi_mlc_content_items VALUES (2, 'faq-returns', 'faq')");
        $this->mysqli->query("INSERT INTO mi_mlc_content_items VALUES (3, 'blog-launch', 'article')");

        // Translations for welcome-page: en, ja, fr
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (1, 1, 'en', 'Welcome', 'Welcome to our site')");
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (2, 1, 'ja', 'ようこそ', 'サイトへようこそ')");
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (3, 1, 'fr', 'Bienvenue', 'Bienvenue sur notre site')");

        // Translations for faq-returns: en, ja
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (4, 2, 'en', 'Return Policy', 'You can return within 30 days')");
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (5, 2, 'ja', '返品ポリシー', '30日以内に返品可能です')");

        // Translations for blog-launch: en only
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (6, 3, 'en', 'Launch Day', 'We are live!')");
    }

    /**
     * Direct translation lookup: SELECT WHERE lang_code='ja' for welcome-page.
     */
    public function testDirectTranslation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.title, t.body
             FROM mi_mlc_translations t
             JOIN mi_mlc_content_items c ON c.id = t.content_id
             WHERE t.lang_code = 'ja' AND c.slug = 'welcome-page'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('ようこそ', $rows[0]['title']);
        $this->assertSame('サイトへようこそ', $rows[0]['body']);
    }

    /**
     * Fallback to default language: LEFT JOIN for 'de' and 'en', COALESCE picks English
     * when no German translation exists for welcome-page.
     */
    public function testFallbackToDefault(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.slug,
                    COALESCE(req.title, def.title) AS title,
                    COALESCE(req.body, def.body) AS body
             FROM mi_mlc_content_items c
             LEFT JOIN mi_mlc_translations req ON req.content_id = c.id AND req.lang_code = 'de'
             LEFT JOIN mi_mlc_translations def ON def.content_id = c.id AND def.lang_code = 'en'
             WHERE c.slug = 'welcome-page'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('welcome-page', $rows[0]['slug']);
        $this->assertSame('Welcome', $rows[0]['title']);
        $this->assertSame('Welcome to our site', $rows[0]['body']);
    }

    /**
     * Fallback for missing language: blog-launch has only English, requesting 'ja'
     * should fall back to 'Launch Day'.
     */
    public function testFallbackForMissingLanguage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.slug,
                    COALESCE(req.title, def.title) AS title,
                    COALESCE(req.body, def.body) AS body
             FROM mi_mlc_content_items c
             LEFT JOIN mi_mlc_translations req ON req.content_id = c.id AND req.lang_code = 'ja'
             LEFT JOIN mi_mlc_translations def ON def.content_id = c.id AND def.lang_code = 'en'
             WHERE c.slug = 'blog-launch'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('blog-launch', $rows[0]['slug']);
        $this->assertSame('Launch Day', $rows[0]['title']);
        $this->assertSame('We are live!', $rows[0]['body']);
    }

    /**
     * Translation coverage stats: COUNT translations per content item.
     */
    public function testTranslationCoverageStats(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.slug, COUNT(t.id) AS translation_count
             FROM mi_mlc_content_items c
             LEFT JOIN mi_mlc_translations t ON t.content_id = c.id
             GROUP BY c.slug
             ORDER BY translation_count DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('welcome-page', $rows[0]['slug']);
        $this->assertEquals(3, (int) $rows[0]['translation_count']);
        $this->assertSame('faq-returns', $rows[1]['slug']);
        $this->assertEquals(2, (int) $rows[1]['translation_count']);
        $this->assertSame('blog-launch', $rows[2]['slug']);
        $this->assertEquals(1, (int) $rows[2]['translation_count']);
    }

    /**
     * All content with French fallback to English via prepared statement.
     */
    public function testAllContentWithLanguage(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.slug,
                    COALESCE(req.title, def.title) AS title,
                    COALESCE(req.body, def.body) AS body
             FROM mi_mlc_content_items c
             LEFT JOIN mi_mlc_translations req ON req.content_id = c.id AND req.lang_code = ?
             LEFT JOIN mi_mlc_translations def ON def.content_id = c.id AND def.lang_code = 'en'
             ORDER BY c.slug",
            ['fr']
        );

        $this->assertCount(3, $rows);

        // blog-launch: no French → fallback to English
        $this->assertSame('blog-launch', $rows[0]['slug']);
        $this->assertSame('Launch Day', $rows[0]['title']);

        // faq-returns: no French → fallback to English
        $this->assertSame('faq-returns', $rows[1]['slug']);
        $this->assertSame('Return Policy', $rows[1]['title']);

        // welcome-page: has French
        $this->assertSame('welcome-page', $rows[2]['slug']);
        $this->assertSame('Bienvenue', $rows[2]['title']);
    }

    /**
     * INSERT a new German translation, then SELECT directly without fallback.
     */
    public function testAddTranslation(): void
    {
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (7, 1, 'de', 'Willkommen', 'Willkommen auf unserer Seite')");

        $rows = $this->ztdQuery(
            "SELECT c.slug,
                    COALESCE(req.title, def.title) AS title,
                    COALESCE(req.body, def.body) AS body
             FROM mi_mlc_content_items c
             LEFT JOIN mi_mlc_translations req ON req.content_id = c.id AND req.lang_code = 'de'
             LEFT JOIN mi_mlc_translations def ON def.content_id = c.id AND def.lang_code = 'en'
             WHERE c.slug = 'welcome-page'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Willkommen', $rows[0]['title']);
        $this->assertSame('Willkommen auf unserer Seite', $rows[0]['body']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_mlc_translations VALUES (7, 1, 'de', 'Willkommen', 'Willkommen auf unserer Seite')");

        // ZTD sees the new translation
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_mlc_translations");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table is empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mlc_translations');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
