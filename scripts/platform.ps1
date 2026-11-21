param([Parameter(Position=0,Mandatory=$true)][string]$Action,
      [Parameter(ValueFromRemainingArguments=$true)][string[]]$Arguments)
$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
& python (Join-Path $ProjectRoot 'scripts/manage.py') $Action @Arguments
exit $LASTEXITCODE
