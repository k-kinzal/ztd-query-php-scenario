<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a survey / poll results workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers response distribution, percentage calculations, cross-tabulation
 * via conditional aggregation, and GROUP BY HAVING patterns.
 * @spec SPEC-10.2.62
 */
class PostgresSurveyResultsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sv_surveys (
                id INTEGER PRIMARY KEY,
                title VARCHAR(255),
                status VARCHAR(20),
                created_at TIMESTAMP
            )',
            'CREATE TABLE pg_sv_questions (
                id INTEGER PRIMARY KEY,
                survey_id INTEGER,
                question_text VARCHAR(255),
                question_type VARCHAR(20)
            )',
            'CREATE TABLE pg_sv_responses (
                id INTEGER PRIMARY KEY,
                question_id INTEGER,
                respondent_id INTEGER,
                answer_text VARCHAR(255),
                submitted_at TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sv_responses', 'pg_sv_questions', 'pg_sv_surveys'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Surveys
        $this->pdo->exec("INSERT INTO pg_sv_surveys VALUES (1, 'Customer Satisfaction', 'active', '2026-01-01 00:00:00')");
        $this->pdo->exec("INSERT INTO pg_sv_surveys VALUES (2, 'Product Feedback', 'closed', '2025-06-01 00:00:00')");

        // Questions for survey 1
        $this->pdo->exec("INSERT INTO pg_sv_questions VALUES (1, 1, 'How satisfied are you?', 'rating')");
        $this->pdo->exec("INSERT INTO pg_sv_questions VALUES (2, 1, 'Would you recommend us?', 'yes_no')");
        $this->pdo->exec("INSERT INTO pg_sv_questions VALUES (3, 1, 'What could we improve?', 'text')");

        // Questions for survey 2
        $this->pdo->exec("INSERT INTO pg_sv_questions VALUES (4, 2, 'Rate the product quality', 'rating')");
        $this->pdo->exec("INSERT INTO pg_sv_questions VALUES (5, 2, 'Would you buy again?', 'yes_no')");

        // Responses for question 1 (satisfaction rating): respondents 1-6
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (1, 1, 1, '5', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (2, 1, 2, '4', '2026-02-01 10:01:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (3, 1, 3, '5', '2026-02-01 10:02:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (4, 1, 4, '3', '2026-02-01 10:03:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (5, 1, 5, '4', '2026-02-01 10:04:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (6, 1, 6, '5', '2026-02-01 10:05:00')");

        // Responses for question 2 (recommend yes/no): respondents 1-6
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (7, 2, 1, 'yes', '2026-02-01 10:10:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (8, 2, 2, 'yes', '2026-02-01 10:11:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (9, 2, 3, 'yes', '2026-02-01 10:12:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (10, 2, 4, 'no', '2026-02-01 10:13:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (11, 2, 5, 'yes', '2026-02-01 10:14:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (12, 2, 6, 'no', '2026-02-01 10:15:00')");

        // Responses for question 3 (text feedback): respondents 1, 2, 4
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (13, 3, 1, 'Nothing', '2026-02-01 10:20:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (14, 3, 2, 'Faster shipping', '2026-02-01 10:21:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (15, 3, 4, 'Better pricing', '2026-02-01 10:22:00')");

        // Responses for question 4 (product quality rating): respondents 1-4
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (16, 4, 1, '5', '2025-07-01 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (17, 4, 2, '4', '2025-07-01 09:01:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (18, 4, 3, '3', '2025-07-01 09:02:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (19, 4, 4, '5', '2025-07-01 09:03:00')");

        // Responses for question 5 (buy again yes/no): respondents 1-4
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (20, 5, 1, 'yes', '2025-07-01 09:10:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (21, 5, 2, 'yes', '2025-07-01 09:11:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (22, 5, 3, 'no', '2025-07-01 09:12:00')");
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (23, 5, 4, 'yes', '2025-07-01 09:13:00')");
    }

    public function testResponseCountPerQuestion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT q.question_text, COUNT(r.id) AS response_count
             FROM pg_sv_questions q
             LEFT JOIN pg_sv_responses r ON r.question_id = q.id
             GROUP BY q.id, q.question_text
             ORDER BY q.id"
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(6, (int) $rows[0]['response_count']);
        $this->assertEquals(6, (int) $rows[1]['response_count']);
        $this->assertEquals(3, (int) $rows[2]['response_count']);
        $this->assertEquals(4, (int) $rows[3]['response_count']);
        $this->assertEquals(4, (int) $rows[4]['response_count']);
    }

    public function testResponseDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.answer_text AS rating, COUNT(*) AS cnt
             FROM pg_sv_responses r
             WHERE r.question_id = 1
             GROUP BY r.answer_text
             ORDER BY r.answer_text DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('5', $rows[0]['rating']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);
        $this->assertSame('4', $rows[1]['rating']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);
        $this->assertSame('3', $rows[2]['rating']);
        $this->assertEquals(1, (int) $rows[2]['cnt']);
    }

    public function testAverageRating(): void
    {
        $rows = $this->ztdQuery(
            "SELECT q.question_text,
                    ROUND(AVG(CAST(r.answer_text AS NUMERIC)), 2) AS avg_rating
             FROM pg_sv_questions q
             JOIN pg_sv_responses r ON r.question_id = q.id
             WHERE q.question_type = 'rating'
             GROUP BY q.id, q.question_text
             ORDER BY q.id"
        );

        $this->assertCount(2, $rows);
        // q1: (5+4+5+3+4+5)/6 = 26/6 ≈ 4.33
        $this->assertEqualsWithDelta(4.33, (float) $rows[0]['avg_rating'], 0.01);
        // q4: (5+4+3+5)/4 = 17/4 = 4.25
        $this->assertEqualsWithDelta(4.25, (float) $rows[1]['avg_rating'], 0.01);
    }

    public function testYesNoPercentage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT q.question_text,
                    COUNT(CASE WHEN r.answer_text = 'yes' THEN 1 END) AS yes_count,
                    COUNT(CASE WHEN r.answer_text = 'no' THEN 1 END) AS no_count,
                    ROUND(COUNT(CASE WHEN r.answer_text = 'yes' THEN 1 END) * 100.0 / COUNT(*), 1) AS yes_pct
             FROM pg_sv_questions q
             JOIN pg_sv_responses r ON r.question_id = q.id
             WHERE q.question_type = 'yes_no'
             GROUP BY q.id, q.question_text
             ORDER BY q.id"
        );

        $this->assertCount(2, $rows);
        // q2: 4 yes, 2 no, 66.7%
        $this->assertEquals(4, (int) $rows[0]['yes_count']);
        $this->assertEquals(2, (int) $rows[0]['no_count']);
        $this->assertEqualsWithDelta(66.7, (float) $rows[0]['yes_pct'], 0.1);
        // q5: 3 yes, 1 no, 75.0%
        $this->assertEquals(3, (int) $rows[1]['yes_count']);
        $this->assertEquals(1, (int) $rows[1]['no_count']);
        $this->assertEqualsWithDelta(75.0, (float) $rows[1]['yes_pct'], 0.1);
    }

    public function testSurveyCompletionRate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.title,
                    COUNT(DISTINCT r.respondent_id) AS respondents,
                    (SELECT COUNT(*) FROM pg_sv_questions WHERE survey_id = s.id) AS question_count
             FROM pg_sv_surveys s
             JOIN pg_sv_questions q ON q.survey_id = s.id
             JOIN pg_sv_responses r ON r.question_id = q.id
             GROUP BY s.id, s.title
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        // Survey 1: 6 respondents, 3 questions
        $this->assertSame('Customer Satisfaction', $rows[0]['title']);
        $this->assertEquals(6, (int) $rows[0]['respondents']);
        $this->assertEquals(3, (int) $rows[0]['question_count']);
        // Survey 2: 4 respondents, 2 questions
        $this->assertSame('Product Feedback', $rows[1]['title']);
        $this->assertEquals(4, (int) $rows[1]['respondents']);
        $this->assertEquals(2, (int) $rows[1]['question_count']);
    }

    public function testFilterBySpecificAnswer(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.respondent_id, r.answer_text, r.submitted_at
             FROM pg_sv_responses r
             WHERE r.question_id = ? AND r.answer_text = ?
             ORDER BY r.respondent_id",
            [2, 'yes']
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(1, (int) $rows[0]['respondent_id']);
        $this->assertEquals(2, (int) $rows[1]['respondent_id']);
        $this->assertEquals(3, (int) $rows[2]['respondent_id']);
        $this->assertEquals(5, (int) $rows[3]['respondent_id']);
    }

    public function testSubmitResponseAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO pg_sv_responses VALUES (24, 3, 5, 'More colors', '2026-03-09 10:00:00')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_sv_responses WHERE question_id = 3"
        );
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery(
            "SELECT answer_text FROM pg_sv_responses WHERE id = 24"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('More colors', $rows[0]['answer_text']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sv_surveys VALUES (3, 'New Survey', 'draft', '2026-03-09 00:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sv_surveys");
        $this->assertSame(3, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_sv_surveys")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
