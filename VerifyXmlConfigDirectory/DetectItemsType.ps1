
param(
[parameter(Mandatory=$true)][Array] $Items
)

class ConfigStyleDetectionException : System.Exception
{
    ConfigStyleDetectionException([String] $Message) : Base($Message) {}
    ConfigStyleDetectionException([String] $Message, [System.Exception] $InnerException) : Base($Message, $InnerException) {}
}

function IsCoreXmlConfig([String] $Path)
# CoreXmlConfig is Hierarchical, Caseinsensitive, Nonredundant, Namespaceless
{
    function WalkOverXmlTree([System.Xml.XmlNode] $root, [String] $dattr)
    {
        $Sections = @{}
        $DAttrOccurrences = 0
        foreach ($Child in $root.ChildNodes)
        {
            if ($Child.NodeType -eq [System.Xml.XmlNodeType]::Element)
            {
                $ChildId = $child.get_name()
                $Attrs = New-Object System.Collections.ArrayList
                foreach ($attr in $Child.Attributes)
                {
                    if ($attr.Name -ceq "xmlns" -or $attr.Name.StartsWith("xmlns:"))
                    {
                        throw [ConfigStyleDetectionException]::new("Found namespace `"$($attr.Name)`"")
                    }
                    elseif ($attr.Name -notin $Attrs)
                    {
                        [void] $Attrs.Add($attr.Name)
                    }
                    else
                    {
                        throw [ConfigStyleDetectionException]::new("Found dublicate attribute `"$($attr.Name)`" in `"$ChildId`" (attributes check)")
                    }
                }
                $i = -1
                foreach($attr in $Attrs)
                {
                    $i += 1
                    if ($dattr -eq $attr)
                    {
                        $ChildId = "$ChildId::$($Child.Attributes.GetNamedItem($attr).Value)"
                        ++ $DAttrOccurrences
                        break
                    }
                    $i += 1
                }
                if ($i -gt -1 -and $i -lt $Attrs.Count)
                {
                    $Attrs.RemoveAt($i)
                }
                if (-not $Sections.Contains($ChildId))
                {
                    $Sections[$ChildId] = $Attrs
                }
                else
                {
                    foreach ($attr in $Attrs)
                    {
                        if ($attr -notin $Sections[$ChildId])
                        {
                            [void] $Sections[$ChildId].Add($attr)
                        }
                        else
                        {
                            throw [ConfigStyleDetectionException]::new("Found dublicate attribute `"$attr`" in `"$ChildId`" (sections check)")
                        }
                    }
                }
                $rv = WalkOverXmlTree $Child $dattr
                $DAttrOccurrences += $rv[1]
            }
        }
        return $DAttrOccurrences
    }

    $KnownDistinguishingAttributes = @("Name", "Key")

    try
    {
        $xml = New-Object System.Xml.XmlDocument
        $xml.Load($Path)
        $xmlRoot = $xml.DocumentElement
        $BestAttr = $null
        $BestAttrScore = -1
        foreach ($attr in $KnownDistinguishingAttributes)
        {
            Write-Verbose "Try DAttr `"$attr`""
            try
            {
                $rv = WalkOverXmlTree $xmlRoot $attr
            }
            catch [ConfigStyleDetectionException]
            {
                Write-Verbose $_
                $rv = $null
            }
            if ($null -ne $rv -and $rv -gt $BestAttrScore)
            {
                Write-Verbose "Success for DAttr `"$attr`" (score is $rv)"
                $BestAttr = $attr
                $BestAttrScore = $rv
            }
        }
        return $BestAttr
    }
    catch [System.Xml.XmlException]
    {
        throw [ConfigStyleDetectionException]::new("Could not process `"$Path`" as XML", $_.Exception)
    }
}

foreach ($Item in $Items)
{
    foreach ($Subitem in $Item.GetEnumerator())
    {
        Write-Verbose "Detect type of  $($Subitem.Value.Item.FullName)"
        $Subitem.Value.ItemType = "Unknown"
        try
        {
            if ($rv = IsCoreXmlConfig $Subitem.Value.Item.FullName)
            {
                Write-Verbose "Type of item is `"$($Subitem.Value.ItemType)`", DAttr is `"$rv`""
                $Subitem.Value.ItemType = "CoreXmlConfig"
                $Subitem.Value.CoreXmlConfigDAttr = $rv
            }
        }
        catch [ConfigStyleDetectionException]
        {
            Write-Host "$($_.Exception.Message), $($_.Exception.InnerException.Message)" -ForegroundColor Black -BackgroundColor Yellow
        }
        Write-Verbose "Type of item is `"$($Subitem.Value.ItemType)`""
        if ("Unknown" -eq $Subitem.Value.ItemType)
        {
            Write-Host "WARNING: Could not detect type of `"$($Subitem.Value.Item.FullName)`"" -ForegroundColor Black -BackgroundColor Yellow
        }
    }
}

return $Items
