Param(
    [string]$EnvFile = ".env",
    [switch]$SkipInstall,
    [switch]$OpenBrowser,
    [switch]$Reload
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Import-DotEnv {
    param(
        [string]$Path
    )

    if (-not (Test-Path -Path $Path)) {
        throw "Environment file not found: $Path"
    }

    foreach ($line in Get-Content -Path $Path) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#")) { continue }
        $parts = $trimmed -split "=", 2
        if ($parts.Length -lt 2) { continue }
        $name = $parts[0].Trim()
        $value = $parts[1].Trim([char]39).Trim([char]34)
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

function Resolve-CommandPath {
    param(
        [string[]]$Candidates
    )

    foreach ($candidate in $Candidates) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    throw "Could not find executable. Tried: $($Candidates -join ', ')"
}

function Clear-ListeningPort {
    param(
        [int]$Port
    )

    $processIds = @()

    $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object { $_.OwningProcess -gt 0 } |
        Select-Object -ExpandProperty OwningProcess -Unique
    if ($connections) {
        $processIds += $connections
    }

    $netstatLines = netstat -ano | Select-String -Pattern (":{0}\s" -f $Port)
    foreach ($line in $netstatLines) {
        $parts = ($line.ToString().Trim() -split "\s+") | Where-Object { $_ }
        if ($parts.Length -ge 5) {
            $pidText = $parts[-1]
            $parsedPid = 0
            if ([int]::TryParse($pidText, [ref]$parsedPid) -and $parsedPid -gt 0) {
                $processIds += $parsedPid
            }
        }
    }

    $processIds = $processIds | Select-Object -Unique

    foreach ($processId in $processIds) {
        try {
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
        catch {
        }
    }

    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $stillBound = netstat -ano | Select-String -Pattern ("127.0.0.1:{0}\s" -f $Port)
        if (-not $stillBound) {
            break
        }
        Start-Sleep -Milliseconds 250
    }
}

Import-DotEnv -Path $EnvFile

$RepoRoot = $PSScriptRoot
$FrontendRoot = Join-Path $RepoRoot "frontend"
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (Test-Path -Path $VenvPython) {
    $PythonExe = $VenvPython
} else {
    $PythonExe = Resolve-CommandPath @("py.exe", "python.exe")
}

$NpmExe = Resolve-CommandPath @(
    "npm.cmd",
    "C:\Program Files\nodejs\npm.cmd",
    "C:\Program Files (x86)\nodejs\npm.cmd"
)

if (-not $SkipInstall) {
    Write-Host "Installing backend and frontend dependencies..."
    & $NpmExe install
    if ($LASTEXITCODE -ne 0) { throw "npm install failed in repo root." }

    & $NpmExe install --prefix $FrontendRoot
    if ($LASTEXITCODE -ne 0) { throw "npm install failed in frontend." }

    & $PythonExe -m pip install -r (Join-Path $RepoRoot "backend\requirements.txt")
    if ($LASTEXITCODE -ne 0) { throw "pip install failed." }
}

Clear-ListeningPort -Port 5000
Clear-ListeningPort -Port 3000

$reloadArgs = @()
$backendMode = "without reload"
if ($Reload) {
    $reloadArgs = @("--reload")
    $backendMode = "with reload"
}

$url = "http://127.0.0.1:3000"
Write-Host "Starting frontend dev server with HMR on $url in a separate terminal"
$escapedRepoRoot = $RepoRoot.Replace('"', '""')
$escapedFrontendRoot = $FrontendRoot.Replace('"', '""')
$frontendCommand = "cd /d `"$escapedFrontendRoot`" && `"$NpmExe`" run dev"
$frontendProcess = Start-Process `
    -FilePath "cmd.exe" `
    -ArgumentList @("/k", $frontendCommand) `
    -WorkingDirectory $FrontendRoot `
    -PassThru

Start-Sleep -Seconds 5

if ($OpenBrowser) {
    Start-Process $url | Out-Null
}

Write-Host "Starting backend $backendMode on http://127.0.0.1:5000 in this terminal"
[Environment]::SetEnvironmentVariable("PYTHONUNBUFFERED", "1", "Process")

try {
    & $PythonExe -u -m backend.app.serve --host 127.0.0.1 --port 5000 @reloadArgs
}
finally {
    if ($frontendProcess -and -not $frontendProcess.HasExited) {
        try {
            taskkill /PID $frontendProcess.Id /T /F | Out-Null
        }
        catch {
        }
    }
}
