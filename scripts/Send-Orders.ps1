#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateRange(1, 100000)]
    [int]$Count = 100,
    [uri]$BaseUrl = 'http://localhost:8080',
    [ValidateRange(1, 999)]
    [int]$Quantity = 1,
    [ValidateRange(0, 60000)]
    [int]$DelayMs = 0,
    [ValidateRange(0, 100000)]
    [int]$FailureEvery = 0,
    [ValidateRange(1, 100)]
    [int]$FailureAttempts = 1,
    [ValidateNotNullOrEmpty()]
    [ValidateLength(1, 80)]
    [string]$ProductPrefix = 'Simulation',
    [ValidateRange(1, 300)]
    [int]$TimeoutSeconds = 30,
    [switch]$ContinueOnError
)

$ErrorActionPreference = 'Stop'
if ($BaseUrl.Scheme -notin @('http', 'https') -or $BaseUrl.Query -or $BaseUrl.Fragment) {
    throw 'BaseUrl must be an HTTP(S) URL without a query string or fragment.'
}
$endpoint = $BaseUrl.AbsoluteUri.TrimEnd('/') + '/orders'
$runId = [guid]::NewGuid().ToString('N').Substring(0, 8)
$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds($TimeoutSeconds)
$timer = [System.Diagnostics.Stopwatch]::StartNew()
$attemptedCount = 0
$createdCount = 0
$failedCount = 0
$lastOrderId = $null

Write-Host "Sending $Count order(s) to $endpoint. Run ID: $runId"
try {
    for ($index = 1; $index -le $Count; $index++) {
        $content = $null
        $response = $null
        $attemptedCount++
        try {
            $payload = @{
                product = "$ProductPrefix $runId #$index"
                quantity = $Quantity
                simulateFailure = ($FailureEvery -gt 0 -and $index % $FailureEvery -eq 0)
                failureAttempts = $FailureAttempts
            } | ConvertTo-Json -Compress
            $content = [System.Net.Http.StringContent]::new($payload,
                [System.Text.Encoding]::UTF8, 'application/json')
            # Reutiliza a conexão e envia uma requisição por vez, sem guardar uma lista de eventos.
            $response = $client.PostAsync($endpoint, $content).GetAwaiter().GetResult()
            $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            if ([int]$response.StatusCode -ne 201) {
                throw "Expected HTTP 201; received HTTP $([int]$response.StatusCode). $body"
            }
            $order = $body | ConvertFrom-Json
            $lastOrderId = $order.id
            $createdCount++
        } catch {
            $failedCount++
            # Um timeout pode ocorrer depois do commit. Repetir sem idempotência poderia duplicar o pedido.
            Write-Warning "Request $index failed or has an unknown result. No automatic retry. $($_.Exception.Message)"
            if (-not $ContinueOnError) { break }
        } finally {
            if ($null -ne $response) { $response.Dispose() }
            if ($null -ne $content) { $content.Dispose() }
        }
        if ($index -eq 1 -or $index % 100 -eq 0 -or $index -eq $Count) {
            Write-Progress -Activity 'Sending orders' -Status "$index / $Count; created: $createdCount" -PercentComplete ([int]($index * 100.0 / $Count))
        }
        if ($DelayMs -gt 0 -and $index -lt $Count) { Start-Sleep -Milliseconds $DelayMs }
    }
} finally {
    $timer.Stop()
    $client.Dispose()
    Write-Progress -Activity 'Sending orders' -Completed
}

# O resumo ocupa memória constante, independentemente da quantidade solicitada.
[pscustomobject]@{
    RunId = $runId
    Requested = $Count
    Attempted = $attemptedCount
    Created = $createdCount
    FailedOrUnknown = $failedCount
    ElapsedSeconds = [math]::Round($timer.Elapsed.TotalSeconds, 2)
    CreatedPerSecond = [math]::Round($createdCount / [math]::Max(0.001, $timer.Elapsed.TotalSeconds), 2)
    LastOrderId = $lastOrderId
    FinishedEarly = ($attemptedCount -lt $Count)
}
if ($failedCount -gt 0) {
    throw "$failedCount request(s) failed or have an unknown result. Check the dashboard before resubmitting."
}
