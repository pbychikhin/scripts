
param(
[parameter(Mandatory=$true)][string] $SourcePath,
[parameter(Mandatory=$true)][string] $DestPath,
[Hashtable] $Switches = $null,
[Switch] $ReportMode
)

if ($Switches.ReportMode) { $ReportMode = $true }

function UpdateDirectoryContents($SourcePath, $DestPath, $ReportMode=$false, $FoundItems=$null)
{
    if ($null -eq $FoundItems)
    {
        $FoundItems = New-Object System.Collections.ArrayList
    }
    $SourceDirList = @(Get-ChildItem $SourcePath)
    $DestDirList = @(Get-ChildItem $DestPath)
    foreach ($item in $SourceDirList)
    {
        $DestItem = ($DestDirList | Where-Object {$_.Name -eq $item.Name})
        if ($DestItem)
        {
            if ($item.Attributes -band [System.IO.FileAttributes]::Directory)
            {
                $FoundItems = UpdateDirectoryContents $item.FullName $DestItem.FullName $ReportMode $FoundItems
            }
            else
            {
                [Void] $FoundItems.Add(@{SourceItem=@{Item=$item}; DestItem= @{Item=$DestItem}})
            }
        }
        else
        {
            if (-not $ReportMode)
            {
                Write-Host "Copy `"$($item.FullName)`" to `"$DestPath`"" -ForegroundColor Yellow -BackgroundColor DarkGreen
                Copy-Item $item.FullName $DestPath -Recurse
            }
            else
            {
                Write-Host "Need to copy `"$($item.FullName)`" to `"$DestPath`"" -ForegroundColor Yellow -BackgroundColor DarkGreen
            }
        }
    }
    return , $FoundItems
}

return UpdateDirectoryContents $SourcePath $DestPath $ReportMode
