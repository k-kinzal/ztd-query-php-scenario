#!/usr/bin/env php
<?php

/**
 * Compare two baseline.json files and classify each change.
 *
 * Usage:
 *   php scripts/compare-baseline.php <old-baseline.json> <new-baseline.json> [--format=text|json]
 *   php scripts/compare-baseline.php --help
 *
 * Classifications:
 *   - regression:       test passed before, now fails
 *   - newly-supported:  test failed before, now passes
 *   - intentional:      test result changed and versions differ
 *   - removed:          test existed before, absent now
 *   - added:            test absent before, exists now
 *   - unchanged:        same result
 */

declare(strict_types=1);

if (in_array('--help', $argv, true) || in_array('-h', $argv, true) || $argc < 3) {
    fwrite(STDOUT, <<<'HELP'
    Compare two baseline.json files and classify each change.

    Usage:
      php scripts/compare-baseline.php <old-baseline.json> <new-baseline.json> [--format=text|json]

    Options:
      --format=text   Human-readable summary (default)
      --format=json   Machine-readable JSON output

    Classifications:
      regression       Previously passing test now fails
      newly-supported  Previously failing test now passes
      intentional      Result changed between different ztd/db versions
      removed          Test present in old baseline but absent in new
      added            Test present in new baseline but absent in old
      unchanged        Same result in both baselines

    HELP);
    exit($argc < 3 ? 1 : 0);
}

$oldPath = $argv[1];
$newPath = $argv[2];

$format = 'text';
foreach ($argv as $arg) {
    if (str_starts_with($arg, '--format=')) {
        $format = substr($arg, 9);
    }
}

if (!file_exists($oldPath)) {
    fwrite(STDERR, "Old baseline not found: {$oldPath}\n");
    exit(1);
}
if (!file_exists($newPath)) {
    fwrite(STDERR, "New baseline not found: {$newPath}\n");
    exit(1);
}

$oldData = json_decode(file_get_contents($oldPath), true);
$newData = json_decode(file_get_contents($newPath), true);

if ($oldData === null || $newData === null) {
    fwrite(STDERR, "Failed to parse JSON from one or both baseline files.\n");
    exit(1);
}

$oldTests = indexByClass($oldData['tests'] ?? []);
$newTests = indexByClass($newData['tests'] ?? []);

$allClasses = array_unique(array_merge(array_keys($oldTests), array_keys($newTests)));
sort($allClasses);

$changes = [];

foreach ($allClasses as $class) {
    $old = $oldTests[$class] ?? null;
    $new = $newTests[$class] ?? null;

    if ($old === null) {
        $changes[] = [
            'testClass' => $class,
            'classification' => 'added',
            'oldResult' => null,
            'newResult' => $new['result'],
            'detail' => 'New test class',
        ];
        continue;
    }

    if ($new === null) {
        $changes[] = [
            'testClass' => $class,
            'classification' => 'removed',
            'oldResult' => $old['result'],
            'newResult' => null,
            'detail' => 'Test class removed',
        ];
        continue;
    }

    $oldResult = $old['result'];
    $newResult = $new['result'];

    if ($oldResult === $newResult) {
        $changes[] = [
            'testClass' => $class,
            'classification' => 'unchanged',
            'oldResult' => $oldResult,
            'newResult' => $newResult,
            'detail' => '',
        ];
        continue;
    }

    $versionChanged = ($old['ztdVersion'] ?? '') !== ($new['ztdVersion'] ?? '')
        || ($old['dbVersion'] ?? '') !== ($new['dbVersion'] ?? '');

    if ($oldResult === 'pass' && $newResult === 'fail') {
        $changes[] = [
            'testClass' => $class,
            'classification' => $versionChanged ? 'intentional' : 'regression',
            'oldResult' => $oldResult,
            'newResult' => $newResult,
            'detail' => $versionChanged
                ? sprintf('Version changed: ztd %s→%s, db %s→%s',
                    $old['ztdVersion'] ?? '?', $new['ztdVersion'] ?? '?',
                    $old['dbVersion'] ?? '?', $new['dbVersion'] ?? '?')
                : 'Same versions — likely regression',
        ];
    } elseif ($oldResult === 'fail' && $newResult === 'pass') {
        $changes[] = [
            'testClass' => $class,
            'classification' => 'newly-supported',
            'oldResult' => $oldResult,
            'newResult' => $newResult,
            'detail' => $versionChanged
                ? sprintf('Version changed: ztd %s→%s, db %s→%s',
                    $old['ztdVersion'] ?? '?', $new['ztdVersion'] ?? '?',
                    $old['dbVersion'] ?? '?', $new['dbVersion'] ?? '?')
                : 'Same versions — behavior fixed',
        ];
    }
}

// Output
if ($format === 'json') {
    $nonUnchanged = array_filter($changes, fn(array $c) => $c['classification'] !== 'unchanged');
    echo json_encode([
        'oldBaseline' => $oldPath,
        'newBaseline' => $newPath,
        'summary' => summarize($changes),
        'changes' => array_values($nonUnchanged),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
} else {
    $summary = summarize($changes);
    fwrite(STDOUT, "Baseline comparison: {$oldPath} → {$newPath}\n\n");
    fwrite(STDOUT, sprintf(
        "  %d total | %d unchanged | %d regressions | %d newly-supported | %d intentional | %d added | %d removed\n\n",
        $summary['total'],
        $summary['unchanged'],
        $summary['regression'],
        $summary['newly-supported'],
        $summary['intentional'],
        $summary['added'],
        $summary['removed'],
    ));

    $nonUnchanged = array_filter($changes, fn(array $c) => $c['classification'] !== 'unchanged');
    if (empty($nonUnchanged)) {
        fwrite(STDOUT, "  No changes detected.\n");
    } else {
        foreach ($nonUnchanged as $change) {
            $arrow = match ($change['classification']) {
                'regression' => '✗',
                'newly-supported' => '✓',
                'intentional' => '~',
                'added' => '+',
                'removed' => '-',
                default => '?',
            };
            $detail = $change['detail'] ? " ({$change['detail']})" : '';
            fwrite(STDOUT, sprintf(
                "  [%s] %-20s %s%s\n",
                $arrow,
                $change['classification'],
                $change['testClass'],
                $detail,
            ));
        }
    }

    // Exit with non-zero if regressions found
    if ($summary['regression'] > 0) {
        fwrite(STDOUT, "\n  ⚠ {$summary['regression']} regression(s) detected.\n");
        exit(2);
    }
}

function indexByClass(array $tests): array
{
    $indexed = [];
    foreach ($tests as $test) {
        $indexed[$test['testClass']] = $test;
    }
    return $indexed;
}

function summarize(array $changes): array
{
    $summary = [
        'total' => count($changes),
        'unchanged' => 0,
        'regression' => 0,
        'newly-supported' => 0,
        'intentional' => 0,
        'added' => 0,
        'removed' => 0,
    ];
    foreach ($changes as $change) {
        $key = $change['classification'];
        if (isset($summary[$key])) {
            $summary[$key]++;
        }
    }
    return $summary;
}
