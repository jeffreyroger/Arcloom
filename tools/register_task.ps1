# Arc Loom — register/reconfigure the 15-minute ingestion scheduled task.
#
# Idempotent: safe to re-run after any change. It removes any prior task named
# "ArcLoom" (and the legacy "ArcLoomIngest") and registers a fresh one, then
# prints the resulting configuration for verification.
#
# Uses the ScheduledTasks module (New-ScheduledTask* cmdlets), not schtasks.exe,
# so every setting is explicit and readable.

$ErrorActionPreference = "Stop"

$TaskName = "ArcLoom"
$ProjectDir = "E:\Projects\Arcloom"
$BatchFile = Join-Path $ProjectDir "run_ingest.bat"
$UserId = "$env:USERDOMAIN\$env:USERNAME"   # e.g. laptop-0aduu46h\jeffrey

# --- Elevation guard: registering an S4U ("run whether user is logged on or
#     not") principal requires SeTcbPrivilege, i.e. an elevated session.
#     Fail BEFORE removing the existing task, so a non-elevated run leaves the
#     current soak task intact instead of deleting it and then erroring. ---
$isAdmin = ([Security.Principal.WindowsPrincipal] `
    [Security.Principal.WindowsIdentity]::GetCurrent()
    ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error ("This script must run in an ELEVATED PowerShell to register an S4U task. " +
        "Right-click Windows Terminal / PowerShell -> 'Run as administrator', then re-run:`n" +
        "    & '$($MyInvocation.MyCommand.Path)'")
    exit 1
}

# --- Remove prior tasks so this script is idempotent ---
foreach ($old in @("ArcLoom", "ArcLoomIngest")) {
    $existing = Get-ScheduledTask -TaskName $old -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $old -Confirm:$false
        Write-Output "Removed existing task: $old"
    }
}

# --- Action: run the batch wrapper, which cd's into the project and appends to logs\pipeline.log ---
$action = New-ScheduledTaskAction -Execute $BatchFile -WorkingDirectory $ProjectDir

# --- Trigger: fire once now, then repeat every 15 minutes indefinitely ---
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

# --- Principal: S4U = "run whether user is logged on or not" without storing a
#     password. S4U tokens cannot reach network resources needing the user's
#     credentials, but outbound HTTPS works — all this pipeline needs. ---
$principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType S4U -RunLevel Limited

# --- Settings: survive sleep, battery, and missed occurrences; never overlap;
#     kill a hung run before the next fires. ---
$settings = New-ScheduledTaskSettingsSet `
    -WakeToRun `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask -TaskName $TaskName `
    -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
    -Description "Arc Loom pipeline.run every 15 minutes (soak window)" `
    -Force | Out-Null

Write-Output "Registered task: $TaskName (principal $UserId, LogonType S4U)"
Write-Output ""
Write-Output "=== Settings ==="
Get-ScheduledTask -TaskName $TaskName | Select-Object -ExpandProperty Settings
Write-Output "=== Principal ==="
Get-ScheduledTask -TaskName $TaskName | Select-Object -ExpandProperty Principal
Write-Output "=== Task info ==="
Get-ScheduledTaskInfo -TaskName $TaskName
