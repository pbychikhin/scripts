
param (
    [Parameter(Mandatory=$true)] [string] $ProjectDir,
    [Parameter(Mandatory=$true)] [string] $BranchListFile,
    [string] $Prefix = "origin",
    [switch] $Remove=$false
)

$Git = "git"

$ProjectDir = Resolve-Path $ProjectDir
$BranchListFile = Resolve-Path $BranchListFile

function Refresh {
    & $Git fetch -q
}

function GetRemoteList {
    return $(& $Git branch -lr -q) | ForEach-Object {$_.Trim()}
}

function GetBranchList {
    return $(Get-Content $BranchListFile) | ForEach-Object {$_.Trim()}
}

function CompareLists($RemoteList, $BranchList) {
    $found = [System.Collections.ArrayList] @()
    $notfound = [System.Collections.ArrayList] @()
    foreach ($item in $BranchList) {
        if ($item -cin $RemoteList) {
            [void] $found.Add($item)
        }
        else {
            [void] $notfound.Add($item)
        }
    }
    return $found, $notfound
}

Push-Location
Set-Location $ProjectDir
try {
    Refresh
    $f, $nf = CompareLists $(GetRemoteList) $(GetBranchList)
    if (-not $Remove) {
        "To be removed:"
        $f
    }
    else {
        foreach ($item in $f) {
            $item_parts = $item -split "/"
            if ($item_parts[0] -eq $Prefix) {
                Write-Host "Remove $item"
                & $Git push $Prefix ":$($item_parts[1..$item_parts.Length] -join '/')" -q
            }
            else {
                Write-Host "Unknown prefix in $item"
            }
        }
    }
}
finally {
    Pop-Location
}
