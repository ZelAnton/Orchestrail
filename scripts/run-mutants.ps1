#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs mutation testing for Orchestrail's deterministic algorithmic layers.

.DESCRIPTION
    Runs the clean engine tests twice because cargo-mutants has no
    min_test_passes configuration setting, then runs cargo-mutants with the
    repository configuration. Additional arguments are passed through to
    cargo-mutants.

    Exit 0 means mutation analysis completed; surviving mutants remain
    informational. Setup, configuration, baseline, and internal errors fail.

    Examples:
        ./scripts/run-mutants.ps1
        ./scripts/run-mutants.ps1 --file engine/src/resolvers/**/*.rs
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $CargoMutantsArgs
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$outputDir = Join-Path $repoRoot 'mutants.out'

function Get-NonEmptyLineCount {
    param([Parameter(Mandatory)][string] $Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return 0
    }
    return @(
        Get-Content -LiteralPath $Path | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    ).Count
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
    & cargo test --package orchestrail-engine
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host '==> Verifying the clean engine tests (pass 2 of 2)' -ForegroundColor Cyan
    & cargo test --package orchestrail-engine
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host '==> Running cargo-mutants against configured pure layers' -ForegroundColor Cyan
    & cargo mutants --config .cargo-mutants.toml @CargoMutantsArgs
    $mutantsStatus = $LASTEXITCODE

    $outcomesPath = Join-Path $outputDir 'outcomes.json'
    $mutantsPath = Join-Path $outputDir 'mutants.json'
    if (-not (Test-Path -LiteralPath $outcomesPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $mutantsPath -PathType Leaf)) {
        Write-Error "cargo-mutants did not produce complete results in $outputDir"
        if ($mutantsStatus) { exit $mutantsStatus }
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
