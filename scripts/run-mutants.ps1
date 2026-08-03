#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs mutation testing for Orchestrail's deterministic algorithmic layers.

.DESCRIPTION
    Runs the clean engine tests twice because cargo-mutants has no
    min_test_passes configuration setting, then runs cargo-mutants with the
    repository configuration. Pass --quick for a two-stage smoke run: first
    validate the production .cargo-mutants.toml file list and its explicit
    integration-boundary exclusions, then analyze the narrow tiering resolver
    subset. Other arguments are passed through to cargo-mutants; --list/--json
    are dry-run output overrides and do not satisfy analysis verification
    because they do not produce result reports.

    Exit 0 means mutation analysis completed; surviving mutants remain
    informational. Setup, configuration, baseline, and internal errors fail.

    Examples:
        ./scripts/run-mutants.ps1
        ./scripts/run-mutants.ps1 --quick
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $CargoMutantsArgs
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$outputDir = Join-Path $repoRoot 'mutants.out'
$configFile = '.cargo-mutants.toml'
$runLabel = 'configured pure layers'
$quickMode = $false

if ($CargoMutantsArgs.Count -gt 0 -and $CargoMutantsArgs[0] -eq '--quick') {
    $CargoMutantsArgs = @($CargoMutantsArgs | Select-Object -Skip 1)
    $configFile = '.cargo-mutants-quick.toml'
    $runLabel = 'quick tiering resolver subset'
    $quickMode = $true
}

if ($CargoMutantsArgs | Where-Object { $_ -in '--list', '--list-files', '--json' }) {
    Write-Error 'cargo-mutants dry-run output modes cannot verify analysis reports'
    exit 1
}

function Get-NonEmptyLineCount {
    param([Parameter(Mandatory)][string] $Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return 0
    }
    return @(
        Get-Content -LiteralPath $Path | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    ).Count
}

function Invoke-CleanTests {
    if ($quickMode) {
        & cargo test --package orchestrail-engine --lib -- resolvers::tiering
    } else {
        & cargo test --package orchestrail-engine
    }
}

function Test-ProductionMutantsConfig {
    $configPath = Join-Path $repoRoot '.cargo-mutants.toml'
    $requiredFile = 'engine/src/resolvers/tiering.rs'
    $excludedFiles = @(
        'engine/src/vcs.rs'
        'engine/src/headless.rs'
        'engine/src/supervise.rs'
        'engine/src/run.rs'
        'engine/src/notification.rs'
        'engine/src/verification.rs'
        'engine/src/legacy_fingerprint.rs'
    )

    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
        Write-Error "production config is missing: $configPath"
        exit 1
    }

    # Parse exclude_globs directly so an integration boundary cannot be silently
    # omitted while examine_globs happens not to select it. This deliberately
    # accepts the simple string-array form used by this repository's TOML config.
    $configLines = Get-Content -LiteralPath $configPath
    $arrayStart = @($configLines | Select-String -Pattern '^\s*exclude_globs\s*=\s*\[\s*$')
    if ($arrayStart.Count -ne 1) {
        Write-Error "could not parse exclude_globs from $configPath"
        exit 1
    }

    $excludeGlobs = [System.Collections.Generic.List[string]]::new()
    $arrayClosed = $false
    for ($index = $arrayStart[0].LineNumber; $index -lt $configLines.Count; $index++) {
        $line = $configLines[$index]
        if ($line -match '^\s*\]\s*$') {
            $arrayClosed = $true
            break
        }
        if ([string]::IsNullOrWhiteSpace($line) -or $line -match '^\s*#') {
            continue
        }
        if ($line -notmatch '^\s*"(?<value>[^"\\]+)"\s*,?\s*(#.*)?$') {
            Write-Error "could not parse exclude_globs from $configPath"
            exit 1
        }
        $excludeGlobs.Add($Matches['value'])
    }
    if (-not $arrayClosed) {
        Write-Error "could not parse exclude_globs from $configPath"
        exit 1
    }
    Write-Host "Validated TOML exclude_globs: $($excludeGlobs.Count) entries"

    foreach ($excludedFile in $excludedFiles) {
        if ($excludedFile -notin $excludeGlobs) {
            Write-Error "production config must explicitly exclude $excludedFile"
            exit 1
        }
        Write-Host "Validated explicit exclusion: $excludedFile"
    }

    # --list-files parses and applies the complete TOML document without
    # launching mutation tests. The assertions below also prove a deterministic
    # target remains selected and every external boundary remains absent.
    $listedFiles = @(& cargo mutants --config .cargo-mutants.toml --list-files 2>&1)
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $listedFiles | ForEach-Object { Write-Host $_ }
    $normalizedFiles = @($listedFiles | ForEach-Object { "$($_)".Replace('\', '/') })

    if (-not ($normalizedFiles | Where-Object { $_.Contains($requiredFile) })) {
        Write-Error "production config did not examine $requiredFile"
        exit 1
    }
    foreach ($excludedFile in $excludedFiles) {
        if ($normalizedFiles | Where-Object { $_.Contains($excludedFile) }) {
            Write-Error "production config unexpectedly examined $excludedFile"
            exit 1
        }
    }
    Write-Host 'Validated cargo-mutants production configuration'
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Error 'cargo is not on PATH'
    exit 1
}

& cargo mutants --version *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo-mutants is not installed; run 'cargo install --locked cargo-mutants'"
    exit 1
}

Push-Location $repoRoot
try {
    if ($quickMode) {
        Write-Host '==> Validating production cargo-mutants configuration and boundary exclusions' -ForegroundColor Cyan
        Test-ProductionMutantsConfig
    }

    Write-Host '==> Verifying the clean engine tests (pass 1 of 2)' -ForegroundColor Cyan
    Invoke-CleanTests
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host '==> Verifying the clean engine tests (pass 2 of 2)' -ForegroundColor Cyan
    Invoke-CleanTests
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host "==> Running cargo-mutants against $runLabel" -ForegroundColor Cyan
    & cargo mutants --config $configFile @CargoMutantsArgs
    $mutantsStatus = $LASTEXITCODE

    $outcomesPath = Join-Path $outputDir 'outcomes.json'
    $mutantsPath = Join-Path $outputDir 'mutants.json'
    if (-not (Test-Path -LiteralPath $outcomesPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $mutantsPath -PathType Leaf)) {
        Write-Error "cargo-mutants did not produce complete results in $outputDir"
        # Listing/JSON-only invocations are dry runs and must not masquerade as
        # completed analysis, even if cargo-mutants itself returned success.
        exit 1
    }

    $generatedMutants = @(Get-Content -Raw -LiteralPath $mutantsPath | ConvertFrom-Json).Count
    $caught = Get-NonEmptyLineCount (Join-Path $outputDir 'caught.txt')
    $survived = Get-NonEmptyLineCount (Join-Path $outputDir 'missed.txt')
    $unviable = Get-NonEmptyLineCount (Join-Path $outputDir 'unviable.txt')
    $timeouts = Get-NonEmptyLineCount (Join-Path $outputDir 'timeout.txt')
    $viable = $caught + $survived
    $survivalRate = if ($viable -gt 0) {
        (100 * $survived / $viable).ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
    } else {
        '0.00'
    }

    $summary = @(
        'Mutation testing summary'
        "  Total mutants generated: $generatedMutants"
        "  Caught by tests:         $caught"
        "  Surviving mutants:       $survived"
        "  Survival rate:           $survivalRate% (surviving / viable)"
        "  Unviable mutants:        $unviable"
        "  Timed-out mutants:       $timeouts"
        ''
        "Detailed results: $outputDir"
        'Inspect missed.txt for survivors, outcomes.json for machine-readable results,'
        "and diff/ plus log/ for each mutation's source change and test output."
    )
    $summary | ForEach-Object { Write-Host $_ }
    $summary | Set-Content -LiteralPath (Join-Path $outputDir 'summary.txt')

    # cargo-mutants uses 2 for survivors and 3 for timeouts. Both mean the
    # analysis completed and are informational for this project.
    if ($mutantsStatus -in 0, 2, 3) { exit 0 }
    exit $mutantsStatus
}
finally {
    Pop-Location
}
