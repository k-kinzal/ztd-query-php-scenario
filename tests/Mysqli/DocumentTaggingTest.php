<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a document tagging system workflow through ZTD shadow store (MySQLi).
 * Covers many-to-many junction table operations, tag cloud aggregation,
 * intersection search via HAVING COUNT, untagged document detection, and physical isolation.
 * @spec SPEC-10.2.84
 */
class DocumentTaggingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_dt_documents (
                id INT PRIMARY KEY,
                title VARCHAR(255),
                content VARCHAR(1000),
                created_at DATETIME
            )',
            'CREATE TABLE mi_dt_tags (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )',
            'CREATE TABLE mi_dt_document_tags (
                document_id INT,
                tag_id INT,
                PRIMARY KEY (document_id, tag_id)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_dt_document_tags', 'mi_dt_tags', 'mi_dt_documents'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 documents
        $this->mysqli->query("INSERT INTO mi_dt_documents VALUES (1, 'PHP Basics', 'Introduction to PHP', '2026-01-10 08:00:00')");
        $this->mysqli->query("INSERT INTO mi_dt_documents VALUES (2, 'MySQL Guide', 'Working with MySQL', '2026-01-15 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_dt_documents VALUES (3, 'REST API Design', 'Building REST APIs', '2026-02-01 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_dt_documents VALUES (4, 'Untagged Doc', 'This has no tags', '2026-02-20 11:00:00')");

        // 4 tags
        $this->mysqli->query("INSERT INTO mi_dt_tags VALUES (1, 'php')");
        $this->mysqli->query("INSERT INTO mi_dt_tags VALUES (2, 'database')");
        $this->mysqli->query("INSERT INTO mi_dt_tags VALUES (3, 'api')");
        $this->mysqli->query("INSERT INTO mi_dt_tags VALUES (4, 'tutorial')");

        // Tag assignments: doc1=[php, tutorial], doc2=[database, php], doc3=[api, php]
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (1, 1)");
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (1, 4)");
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (2, 2)");
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (2, 1)");
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (3, 3)");
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (3, 1)");
    }

    /**
     * Tag a document by inserting into the junction table; verify via JOIN.
     */
    public function testTagDocument(): void
    {
        // Tag doc4 with 'tutorial'
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (4, 4)");

        $rows = $this->ztdQuery(
            "SELECT d.title, t.name AS tag_name
             FROM mi_dt_documents d
             JOIN mi_dt_document_tags dt ON dt.document_id = d.id
             JOIN mi_dt_tags t ON t.id = dt.tag_id
             WHERE d.id = 4"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Untagged Doc', $rows[0]['title']);
        $this->assertSame('tutorial', $rows[0]['tag_name']);
    }

    /**
     * Find documents by tag name using a 3-table JOIN with prepared statement.
     */
    public function testDocumentsByTag(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.id, d.title
             FROM mi_dt_documents d
             JOIN mi_dt_document_tags dt ON dt.document_id = d.id
             JOIN mi_dt_tags t ON t.id = dt.tag_id
             WHERE t.name = ?
             ORDER BY d.id",
            ['php']
        );

        // doc1, doc2, doc3 are all tagged 'php'
        $this->assertCount(3, $rows);
        $this->assertSame('PHP Basics', $rows[0]['title']);
        $this->assertSame('MySQL Guide', $rows[1]['title']);
        $this->assertSame('REST API Design', $rows[2]['title']);
    }

    /**
     * Tag cloud: COUNT documents per tag, ordered by popularity descending.
     */
    public function testTagCloudAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name, COUNT(dt.document_id) AS doc_count
             FROM mi_dt_tags t
             LEFT JOIN mi_dt_document_tags dt ON dt.tag_id = t.id
             GROUP BY t.id, t.name
             ORDER BY doc_count DESC, t.name ASC"
        );

        $this->assertCount(4, $rows);

        // php: 3 documents (most popular)
        $this->assertSame('php', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['doc_count']);

        // api: 1, database: 1, tutorial: 1
        $this->assertEquals(1, (int) $rows[1]['doc_count']);
        $this->assertEquals(1, (int) $rows[2]['doc_count']);
        $this->assertEquals(1, (int) $rows[3]['doc_count']);
    }

    /**
     * Intersection search: find documents tagged with BOTH 'php' AND 'database'.
     */
    public function testDocumentsWithAllTags(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title
             FROM mi_dt_documents d
             JOIN mi_dt_document_tags dt ON dt.document_id = d.id
             JOIN mi_dt_tags t ON t.id = dt.tag_id
             WHERE t.name IN ('php', 'database')
             GROUP BY d.id, d.title
             HAVING COUNT(DISTINCT dt.tag_id) = 2"
        );

        // Only doc2 (MySQL Guide) has both 'php' and 'database'
        $this->assertCount(1, $rows);
        $this->assertSame('MySQL Guide', $rows[0]['title']);
    }

    /**
     * Remove a tag from a document and verify the count changes.
     */
    public function testRemoveTag(): void
    {
        // Verify doc1 has 2 tags before removal
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS tag_count FROM mi_dt_document_tags WHERE document_id = 1"
        );
        $this->assertEquals(2, (int) $rows[0]['tag_count']);

        // Remove 'tutorial' (tag_id=4) from doc1
        $this->mysqli->query("DELETE FROM mi_dt_document_tags WHERE document_id = 1 AND tag_id = 4");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // Verify doc1 now has 1 tag
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS tag_count FROM mi_dt_document_tags WHERE document_id = 1"
        );
        $this->assertEquals(1, (int) $rows[0]['tag_count']);
    }

    /**
     * Find documents with no tags via LEFT JOIN ... IS NULL.
     */
    public function testUntaggedDocuments(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title
             FROM mi_dt_documents d
             LEFT JOIN mi_dt_document_tags dt ON dt.document_id = d.id
             WHERE dt.document_id IS NULL
             ORDER BY d.id"
        );

        // Only doc4 has no tags
        $this->assertCount(1, $rows);
        $this->assertSame('Untagged Doc', $rows[0]['title']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_dt_document_tags VALUES (4, 3)");
        $this->mysqli->query("DELETE FROM mi_dt_document_tags WHERE document_id = 1 AND tag_id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_dt_document_tags");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_dt_document_tags');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
