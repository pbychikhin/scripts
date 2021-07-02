
param(
[parameter(Mandatory=$true)][string] $SourcePath,
[parameter(Mandatory=$true)][string] $DestPath,
[parameter()] [Switch] $ReportMode
)

function UpdateDirectoryContents($SourcePath, $DestPath, $FoundItems=$null)
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
                $FoundItems = UpdateDirectoryContents $item.FullName $DestItem.FullName $FoundItems
            }
            else
            {
                [Void] $FoundItems.Add(@{SourceItem=@{Item=$item}; DestItem= @{Item=$DestItem}})
            }
        }
        else
        {
            Write-Host "Copy `"$($item.FullName)`" to `"$DestPath`"" -ForegroundColor Yellow -BackgroundColor DarkGreen
            if (-not $ReportMode)
            {
                Copy-Item $item.FullName $DestPath -Recurse
            }
        }
    }
    return , $FoundItems
}

return UpdateDirectoryContents $SourcePath $DestPath
