#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs mutation testing for Orchestrail's deterministic algorithmic layers.

.DESCRIPTION
    Runs the clean engine tests twice because cargo-mutants has no
    min_test_passes configuration setting, then runs cargo-mutants with the
    repository configuration. Pass --quick for the proven narrow tiering
    resolver smoke run. Other arguments are passed through to cargo-mutants;
    --list/--json are dry-run output overrides and do not satisfy analysis
    verification because they do not produce result reports.

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
