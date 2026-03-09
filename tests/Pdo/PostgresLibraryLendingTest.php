<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a library book lending scenario through ZTD shadow store (PostgreSQL PDO).
 * Members borrow books with due dates; overdue detection, late fee calculation,
 * and availability tracking exercise multiple chained LEFT JOINs, date-based CASE,
 * COALESCE for defaults, GROUP BY with HAVING on computed expression,
 * SUM CASE for cross-tab counts, COUNT DISTINCT for unique borrower stats,
 * prepared statement for member lookup, and physical isolation check.
 * @spec SPEC-10.2.152
 */
class PostgresLibraryLendingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_lib_books (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                author VARCHAR(100),
                isbn VARCHAR(20),
                category VARCHAR(50),
                daily_fee NUMERIC(10,2)
            )',
            'CREATE TABLE pg_lib_members (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                membership_type VARCHAR(20)
            )',
            'CREATE TABLE pg_lib_loans (
                id SERIAL PRIMARY KEY,
                book_id INT,
                member_id INT,
                borrow_date TEXT,
                due_date TEXT,
                return_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_lib_loans', 'pg_lib_members', 'pg_lib_books'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 6 books
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (1, 'The Great Gatsby', 'Fitzgerald', '978-0-7432-7356-5', 'fiction', 0.50)");
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (2, 'Clean Code', 'Robert Martin', '978-0-13-235088-4', 'technical', 0.75)");
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (3, '1984', 'Orwell', '978-0-452-28423-4', 'fiction', 0.50)");
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (4, 'Design Patterns', 'GoF', '978-0-201-63361-0', 'technical', 0.75)");
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (5, 'Sapiens', 'Harari', '978-0-06-231609-7', 'nonfiction', 0.60)");
        $this->pdo->exec("INSERT INTO pg_lib_books VALUES (6, 'Dune', 'Herbert', '978-0-441-17271-9', 'fiction', 0.50)");

        // 4 members
        $this->pdo->exec("INSERT INTO pg_lib_members VALUES (1, 'Alice', 'alice@example.com', 'premium')");
        $this->pdo->exec("INSERT INTO pg_lib_members VALUES (2, 'Bob', 'bob@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO pg_lib_members VALUES (3, 'Carol', 'carol@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO pg_lib_members VALUES (4, 'Dave', 'dave@example.com', 'premium')");

        // 8 loans
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (1, 1, 1, '2026-01-05', '2026-01-19', '2026-01-18')");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (2, 2, 1, '2026-02-01', '2026-02-15', NULL)");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (3, 3, 2, '2026-01-10', '2026-01-24', '2026-01-30')");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (4, 4, 2, '2026-02-10', '2026-02-24', NULL)");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (5, 5, 3, '2026-01-15', '2026-01-29', '2026-01-28')");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (6, 1, 3, '2026-02-05', '2026-02-19', '2026-02-19')");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (7, 6, 4, '2026-02-20', '2026-03-06', NULL)");
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (8, 3, 1, '2026-02-25', '2026-03-11', NULL)");
    }

    /**
     * 3-table JOIN: books currently borrowed (return_date IS NULL), ordered by due_date.
     * Should return 4 rows (loans 2, 4, 7, 8).
     */
    public function testCurrentlyBorrowedBooks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.title, m.name AS borrower, l.borrow_date, l.due_date
             FROM pg_lib_loans l
             JOIN pg_lib_books b ON b.id = l.book_id
             JOIN pg_lib_members m ON m.id = l.member_id
             WHERE l.return_date IS NULL
             ORDER BY l.due_date"
        );

        $this->assertCount(4, $rows);

        // Loan 2: Clean Code, Alice, due 2026-02-15
        $this->assertSame('Clean Code', $rows[0]['title']);
        $this->assertSame('Alice', $rows[0]['borrower']);
        $this->assertSame('2026-02-15', $rows[0]['due_date']);

        // Loan 4: Design Patterns, Bob, due 2026-02-24
        $this->assertSame('Design Patterns', $rows[1]['title']);
        $this->assertSame('Bob', $rows[1]['borrower']);
        $this->assertSame('2026-02-24', $rows[1]['due_date']);

        // Loan 7: Dune, Dave, due 2026-03-06
        $this->assertSame('Dune', $rows[2]['title']);
        $this->assertSame('Dave', $rows[2]['borrower']);
        $this->assertSame('2026-03-06', $rows[2]['due_date']);

        // Loan 8: 1984, Alice, due 2026-03-11
        $this->assertSame('1984', $rows[3]['title']);
        $this->assertSame('Alice', $rows[3]['borrower']);
        $this->assertSame('2026-03-11', $rows[3]['due_date']);
    }

    /**
     * Overdue detection: books not returned AND due_date < reference date.
     * Loan 8 (due 2026-03-11) is NOT overdue on 2026-03-09. Should return 3 rows.
     */
    public function testOverdueBooks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.title, m.name AS borrower, l.due_date,
                    CASE
                        WHEN l.due_date < '2026-03-09' AND l.return_date IS NULL THEN 'overdue'
                        ELSE 'not_overdue'
                    END AS status
             FROM pg_lib_loans l
             JOIN pg_lib_books b ON b.id = l.book_id
             JOIN pg_lib_members m ON m.id = l.member_id
             WHERE l.return_date IS NULL
               AND l.due_date < '2026-03-09'
             ORDER BY l.due_date"
        );

        $this->assertCount(3, $rows);

        // Loan 2: Clean Code, Alice, due 2026-02-15
        $this->assertSame('Clean Code', $rows[0]['title']);
        $this->assertSame('Alice', $rows[0]['borrower']);
        $this->assertSame('overdue', $rows[0]['status']);

        // Loan 4: Design Patterns, Bob, due 2026-02-24
        $this->assertSame('Design Patterns', $rows[1]['title']);
        $this->assertSame('Bob', $rows[1]['borrower']);
        $this->assertSame('overdue', $rows[1]['status']);

        // Loan 7: Dune, Dave, due 2026-03-06
        $this->assertSame('Dune', $rows[2]['title']);
        $this->assertSame('Dave', $rows[2]['borrower']);
        $this->assertSame('overdue', $rows[2]['status']);
    }

    /**
     * Late fee calculation for returned-late books using date subtraction.
     * Only loan 3 was returned late: 6 days x $0.50 = $3.00.
     */
    public function testLateFeeCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.title, m.name AS borrower, l.due_date, l.return_date,
                    (l.return_date::date - l.due_date::date) AS days_late,
                    (l.return_date::date - l.due_date::date) * b.daily_fee AS late_fee
             FROM pg_lib_loans l
             JOIN pg_lib_books b ON b.id = l.book_id
             JOIN pg_lib_members m ON m.id = l.member_id
             WHERE l.return_date IS NOT NULL
               AND l.return_date > l.due_date
             ORDER BY l.due_date"
        );

        $this->assertCount(1, $rows);

        $this->assertSame('1984', $rows[0]['title']);
        $this->assertSame('Bob', $rows[0]['borrower']);
        $this->assertEquals(6, (int) $rows[0]['days_late']);
        $this->assertEquals(3.00, round((float) $rows[0]['late_fee'], 2));
    }

    /**
     * Book availability: LEFT JOIN to active loans, CASE for status.
     * Books 1, 3, 5 available; books 2, 4, 6 borrowed.
     */
    public function testBookAvailability(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.title, b.author,
                    CASE WHEN l.id IS NULL THEN 'available' ELSE 'borrowed' END AS status
             FROM pg_lib_books b
             LEFT JOIN pg_lib_loans l ON l.book_id = b.id AND l.return_date IS NULL
             ORDER BY b.id"
        );

        $this->assertCount(6, $rows);

        // Book 1: The Great Gatsby - available
        $this->assertSame('The Great Gatsby', $rows[0]['title']);
        $this->assertSame('available', $rows[0]['status']);

        // Book 2: Clean Code - borrowed
        $this->assertSame('Clean Code', $rows[1]['title']);
        $this->assertSame('borrowed', $rows[1]['status']);

        // Book 3: 1984 - borrowed (loan 8 not returned)
        $this->assertSame('1984', $rows[2]['title']);
        $this->assertSame('borrowed', $rows[2]['status']);

        // Book 4: Design Patterns - borrowed
        $this->assertSame('Design Patterns', $rows[3]['title']);
        $this->assertSame('borrowed', $rows[3]['status']);

        // Book 5: Sapiens - available
        $this->assertSame('Sapiens', $rows[4]['title']);
        $this->assertSame('available', $rows[4]['status']);

        // Book 6: Dune - borrowed
        $this->assertSame('Dune', $rows[5]['title']);
        $this->assertSame('borrowed', $rows[5]['status']);
    }

    /**
     * Member borrowing stats: LEFT JOIN members to loans, COUNT total loans,
     * COUNT DISTINCT books, SUM CASE for currently_out.
     */
    public function testMemberBorrowingStats(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.name,
                    COUNT(l.id) AS total_loans,
                    COUNT(DISTINCT l.book_id) AS distinct_books,
                    SUM(CASE WHEN l.return_date IS NULL THEN 1 ELSE 0 END) AS currently_out
             FROM pg_lib_members m
             LEFT JOIN pg_lib_loans l ON l.member_id = m.id
             GROUP BY m.id, m.name
             ORDER BY m.id"
        );

        $this->assertCount(4, $rows);

        // Alice: 3 total, 3 distinct, 2 current
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['total_loans']);
        $this->assertEquals(3, (int) $rows[0]['distinct_books']);
        $this->assertEquals(2, (int) $rows[0]['currently_out']);

        // Bob: 2 total, 2 distinct, 1 current
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['total_loans']);
        $this->assertEquals(2, (int) $rows[1]['distinct_books']);
        $this->assertEquals(1, (int) $rows[1]['currently_out']);

        // Carol: 2 total, 2 distinct, 0 current
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['total_loans']);
        $this->assertEquals(2, (int) $rows[2]['distinct_books']);
        $this->assertEquals(0, (int) $rows[2]['currently_out']);

        // Dave: 1 total, 1 distinct, 1 current
        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertEquals(1, (int) $rows[3]['total_loans']);
        $this->assertEquals(1, (int) $rows[3]['distinct_books']);
        $this->assertEquals(1, (int) $rows[3]['currently_out']);
    }

    /**
     * Category popularity: GROUP BY category, COUNT loans, HAVING >= 2.
     * fiction: 5 loans, technical: 2 loans (nonfiction: 1 loan filtered out).
     */
    public function testCategoryPopularity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.category, COUNT(l.id) AS loan_count
             FROM pg_lib_books b
             JOIN pg_lib_loans l ON l.book_id = b.id
             GROUP BY b.category
             HAVING COUNT(l.id) >= 2
             ORDER BY loan_count DESC"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('fiction', $rows[0]['category']);
        $this->assertEquals(5, (int) $rows[0]['loan_count']);

        $this->assertSame('technical', $rows[1]['category']);
        $this->assertEquals(2, (int) $rows[1]['loan_count']);
    }

    /**
     * Prepared statement: find all loans for a given member_id, JOIN with book title.
     * Test with member_id=1 (Alice), should return 3 rows.
     */
    public function testPreparedMemberLoans(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT l.id AS loan_id, b.title, l.borrow_date, l.due_date, l.return_date
             FROM pg_lib_loans l
             JOIN pg_lib_books b ON b.id = l.book_id
             WHERE l.member_id = ?
             ORDER BY l.borrow_date",
            [1]
        );

        $this->assertCount(3, $rows);

        $this->assertSame('The Great Gatsby', $rows[0]['title']);
        $this->assertSame('2026-01-18', $rows[0]['return_date']);

        $this->assertSame('Clean Code', $rows[1]['title']);
        $this->assertNull($rows[1]['return_date']);

        $this->assertSame('1984', $rows[2]['title']);
        $this->assertNull($rows[2]['return_date']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new loan via shadow
        $this->pdo->exec("INSERT INTO pg_lib_loans VALUES (9, 5, 4, '2026-03-09', '2026-03-23', NULL)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_lib_loans");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_lib_loans")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
