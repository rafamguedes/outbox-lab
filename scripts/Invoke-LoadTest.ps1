#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateRange(1, 50000)] [int]$Rate = 50000,
    [ValidateRange(1, 3600)] [int]$DurationSeconds = 2,
    [uri]$BaseUrl = 'http://localhost:8080',
    [ValidateRange(1, 10000)] [int]$PreAllocatedVUs = 32,
    [ValidateRange(1, 10000)] [int]$MaxVUs = 128,
    [ValidateRange(1, 300)] [int]$TimeoutSeconds = 3,
    [ValidateRange(1, 999)] [int]$Quantity = 1,
    [ValidateRange(0, 100000)] [int]$FailureEvery = 0,
    [ValidateRange(1, 100)] [int]$FailureAttempts = 1,
    [ValidateNotNullOrEmpty()] [ValidateLength(1, 60)] [string]$ProductPrefix = 'Load',
    [string]$K6Path = 'k6',
    [string]$ReportsDirectory = (Join-Path $PSScriptRoot '../reports/load'),
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
if ($BaseUrl.Scheme -notin @('http', 'https') -or !$BaseUrl.Host -or $BaseUrl.UserInfo -or $BaseUrl.Query -or $BaseUrl.Fragment) {
    throw 'BaseUrl must be an HTTP(S) URL without credentials, a query, or a fragment.'
}
$plannedRequests = [long]$Rate * $DurationSeconds
if ($plannedRequests -gt 100000) { throw 'Rate * DurationSeconds must not exceed 100000 requests.' }
if ($PreAllocatedVUs -gt $MaxVUs) { throw 'PreAllocatedVUs must not exceed MaxVUs.' }
if ([string]::IsNullOrWhiteSpace($ProductPrefix)) { throw 'ProductPrefix cannot be blank.' }
$runId = [guid]::NewGuid().ToString('N')
$configuration = [ordered]@{
    runId = $runId; baseUrl = $BaseUrl.AbsoluteUri.TrimEnd('/'); targetRps = $Rate
    durationSeconds = $DurationSeconds; plannedRequests = $plannedRequests
    preAllocatedVUs = $PreAllocatedVUs; maxVUs = $MaxVUs; timeoutSeconds = $TimeoutSeconds
    quantity = $Quantity; failureEvery = $FailureEvery; failureAttempts = $FailureAttempts
    productPrefix = $ProductPrefix
}
if ($DryRun) { [pscustomobject]$configuration; return }

$engine = Get-Command $K6Path -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -eq $engine) { throw 'k6 was not found. Install Grafana k6 or provide -K6Path with its executable path.' }
$runDirectory = Join-Path ([IO.Path]::GetFullPath($ReportsDirectory)) $runId
New-Item -ItemType Directory -Path $runDirectory | Out-Null
$summaryPath = Join-Path $runDirectory 'summary.json'
$configuration | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDirectory 'configuration.json')
$environment = [ordered]@{
    RATE = $Rate; DURATION_SECONDS = $DurationSeconds; BASE_URL = $configuration.baseUrl
    PRE_ALLOCATED_VUS = $PreAllocatedVUs; MAX_VUS = $MaxVUs; TIMEOUT_SECONDS = $TimeoutSeconds
    QUANTITY = $Quantity; FAILURE_EVERY = $FailureEvery; FAILURE_ATTEMPTS = $FailureAttempts
    PRODUCT_PREFIX = $ProductPrefix; RUN_ID = $runId; SUMMARY_PATH = $summaryPath
}
$arguments = [Collections.Generic.List[string]]::new()
$arguments.Add('run')
$arguments.Add('--no-usage-report')
$arguments.Add('--no-color')
foreach ($entry in $environment.GetEnumerator()) {
    $arguments.Add('--env')
    $arguments.Add("$($entry.Key)=$($entry.Value)")
}
$arguments.Add((Join-Path $PSScriptRoot 'load/orders.js'))
Write-Host "Target: $Rate requests/s for $DurationSeconds s ($plannedRequests requests). Max VUs: $MaxVUs."
Write-Host "Reports: $runDirectory"
# Os argumentos são passados diretamente, sem montar ou avaliar código de shell.
& $engine.Source @arguments
$engineExit = $LASTEXITCODE
if (!(Test-Path -LiteralPath $summaryPath)) { throw "k6 exited with code $engineExit without a summary. See its output above." }
$report = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
Write-Host "Summary: $summaryPath"
if ($engineExit -ne 0 -or $report.passed -ne $true) {
    throw "Load target was not met. k6 exit code: $engineExit. Created: $($report.createdOrders)/$plannedRequests; dropped: $($report.droppedIterations). See summary.json."
}
