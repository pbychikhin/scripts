
param(
    [Parameter(
            Mandatory=$true,
            Position=0,
            HelpMessage="Source XML config file path",
            ParameterSetName="Normal")] [String] $Source,
    [Parameter(
            Mandatory=$true,
            Position=1,
            HelpMessage="Destination XML config file path",
            ParameterSetName="Normal")] [String] $Destination,
    [Parameter(
            Mandatory=$true,
            Position=2,
            HelpMessage="Updater Id",
            ParameterSetName="Normal")] [String] $UpdaterId,
    [Parameter(
            Position=3,
            HelpMessage="Distinguishing attribute (Name)",
            ParameterSetName="Normal")] [String] $Attribute = "Name",
    [Parameter(
            Position=4,
            HelpMessage="Hashtable of switches",
            ParameterSetName="Normal")] [Hashtable] $Switches = $null,
    [Parameter(
            HelpMessage="Report mode (not to update anything, just report)",
            ParameterSetName="Normal")] [Switch] $ReportMode,
    [Parameter(
            HelpMessage="Don't backup destination file before updating",
            ParameterSetName="Normal")] [Switch] $DontBackup,
    [Parameter(
            HelpMessage="Quiet operation, no confirmation prompts",
            ParameterSetName="Normal")] [Switch] $Quiet,
    [Parameter(
            HelpMessage="Don't set Powershell's Stop preferences (for better debugging)",
            ParameterSetName="Normal")] [Switch] $DontStop,
    [Parameter(
            HelpMessage="Print script's version and exit",
            ParameterSetName="Version")] [Switch] $Version
)

if ($Switches.ReportMode) { $ReportMode = $true }
if ($Switches.DontBackup) { $DontBackup = $true }
if ($Switches.Quiet) { $Quiet = $true }
if ($Switches.DontStop) { $DontStop = $true }

if (-not $DontStop) {
    $ErrorActionPreference = "Stop"
    $PSDefaultParameterValues["*:ErrorAction"] = "Stop"
    $ProgressPreference="SilentlyContinue"
}

$ThisScriptVersion = "0.1.1"
$Timestamp = (Get-Date -Format "yyyyMMddHHmmssff")

if ($Version) {
    Write-Host $ThisScriptVersion
    exit
}

$Config = @{
    DistinguishingAttribute = $Attribute
    Quiet = $Quiet
}

enum PathElementType
{
    EmptyRoot
    Node
    Attribute
    Text
}

class PathConstructor
{
    [Array] $Path
    [String] $DAttr

    PathConstructor()
    {
        $this.Path = @(
            @{
                Element = ""
                ElementType = [PathElementType]::EmptyRoot
            }
        )
        $this.DAttr = "Name"
    }

    PathConstructor([String] $N)
    {
        $this.Path = @(
            @{
                Element = $N
                ElementType = [PathElementType]::Node
            }
        )
        $this.DAttr = "Name"
    }

    PathConstructor([String] $N, [Hashtable] $C)
    {
        $this.Path = @(
            @{
                Element = $N
                ElementType = [PathElementType]::Node
            }
        )
        $this.DAttr = $C.DistinguishingAttribute
    }

    PathConstructor([PathConstructor] $P)
    {
        $this.Path = $P.Path
        $this.DAttr = $P.DAttr
    }

    AddNode([String] $Name)
    {
        $this.Path +=
        @{
            Element = $Name
            ElementType = [PathElementType]::Node
        }
    }

    AddAttributes([Array] $Attributes)
    {
        if ($Attributes.Count)
        {
            $NameAttribute = $null
            $RegularAttributes = @()
            foreach ($item in $Attributes)
            {
                if ($item.Name -eq $this.DAttr)
                {
                    $NameAttribute = $item
                }
                else
                {
                    $RegularAttributes += $item
                }
            }
            $this.Path +=
            @{
                Element =
                @{
                    NameAttribute = $NameAttribute
                    RegularAttributes = $RegularAttributes
                }
                ElementType = [PathElementType]::Attribute
            }
        }
    }

    AddText([String] $Text)
    {
        $this.Path +=
        @{
            Element = $Text
            ElementType = [PathElementType]::Text
        }
    }

    [String] GetPath()
    {
        $Result = "/"
        $NodeCount = 0
        foreach ($item in $this.Path)
        {
            if ($item.ElementType -eq [PathElementType]::EmptyRoot)
            {
                continue
            }
            elseif ($item.ElementType -eq [PathElementType]::Node)
            {
                if ($NodeCount -gt 0)
                {
                    $Result += "/$($item.Element)"
                }
                else
                {
                    $Result += "$($item.Element)"
                }
                $NodeCount += 1
            }
            elseif ($item.ElementType -eq [PathElementType]::Attribute)
            {
                $Attrs = @()
                if ($null -ne $item.Element.NameAttribute)
                {
                    $Attrs += "@$($item.Element.NameAttribute.Name)=`"$($item.Element.NameAttribute.Value)`""
                }
                foreach ($subitem in $item.Element.RegularAttributes)
                {
                    $Attrs += "@$($subitem.Name)"
                }
                $AttrsStr = $Attrs -join " and "
                $Result += "[$AttrsStr]"
            }
            elseif ($item.ElementType -eq [PathElementType]::Text)
            {
                if ($NodeCount -gt 0)
                {
                    $Result += "/text()"
                }
                else
                {
                    $Result += "text()"
                }
                $NodeCount += 1
            }
        }
        return $Result
    }
}

function CreateChildInfo([System.Xml.XmlNode] $child, [Hashtable] $Config)
{
    $ChildId = $child.get_name()
    $Attributes = @()
    foreach ($attr in $child.Attributes)
    {
        if ($Config.DistinguishingAttribute -eq $attr.Name)
        {
            $ChildId = "$($ChildId)::$($attr.Value)"
        }
        else
        {
            $Attributes += $attr
        }
    }
    return @{
        Id = $ChildId
        Attributes = $Attributes
        Node = $child
    }
}

function IncorporateChild([System.Collections.Specialized.OrderedDictionary] $Children, [System.Xml.XmlNode] $Child, [int] $depth, [Hashtable] $Config)
{
    $ChildInfo = CreateChildInfo $Child $Config
    if (-not $Children.Contains($ChildInfo.Id))
    {
        Write-Verbose "$($depth) $(" " * $depth * 3) Create new child <$($ChildInfo.Id)>"
        $Children[$ChildInfo.Id] = $ChildInfo.Node
    }
    else
    {
        Write-Verbose "$($depth) $(" " * $depth * 3) Incorporate child <$($ChildInfo.Id)> into existing one"
        foreach ($attr in $ChildInfo.Attributes)
        {
            [Void] $Children[$ChildInfo.Id].Attributes.Append($attr)
        }
        $Nodes = @()
        foreach ($node in $ChildInfo.Node.ChildNodes)
        {
            $Nodes += $node
        }
        foreach ($node in $Nodes)
        {
            [Void] $Children[$ChildInfo.Id].AppendChild($node)
        }
    }
}

function CompressXmlTree([System.Xml.XmlNode] $root, [Hashtable] $Config, [int] $depth=0)
{
    foreach ($Child in $root.ChildNodes)
    {
        CompressXmlTree $Child $Config ($depth + 1)
    }
    $IncorporatedChildren = [Ordered] @{}
    $Children = @()
    foreach ($Child in $root.ChildNodes)
    {
        if ($Child.NodeType -in ([System.Xml.XmlNodeType]::Element, [System.Xml.XmlNodeType]::Text))
        {
            Write-Verbose "$($depth) $("." * $depth * 3) Incorporate child <$($Child.get_name())> $($Child.Value)"
            IncorporateChild $IncorporatedChildren $Child $depth $Config
        }
        $Children += $Child
    }
    foreach ($Child in $Children)
    {
        [Void] $root.RemoveChild($Child)
    }
    foreach ($Child in $IncorporatedChildren.GetEnumerator())
    {
        [Void] $root.AppendChild($Child.Value)
    }
}

function CreateChildInfoTable($ChildList, $Config)
{
    $ChildInfoTable = @{}
    foreach ($child in $ChildList)
    {
        $ChildInfo = CreateChildInfo $child $Config
        $ChildInfoTable[$ChildInfo.Id] = $ChildInfo
    }
    return $ChildInfoTable
}

function CompareXmlTree([System.Xml.XmlNode] $root1, [System.Xml.XmlNode] $root2, [PathConstructor] $path, [Array] $result, [Hashtable] $Config, [int] $depth=0)
{
    Write-Verbose "$($depth) $("." * $depth * 3) At path $($path.GetPath())"
    $Root2ChildInfoTable = CreateChildInfoTable $root2.ChildNodes $Config
    foreach ($Root1ChildInfo in ($root1.ChildNodes | ForEach-Object {CreateChildInfo $_ $Config}))
    {
        Write-Verbose "$($depth) $(" " * $depth * 3) Process child <$($Root1ChildInfo.Id)>"
        if ($Root2ChildInfoTable.Contains($Root1ChildInfo.Id))
        {
            $Root2ChildInfo = $Root2ChildInfoTable[$Root1ChildInfo.Id]
            Write-Verbose "$($depth) $(" " * $depth * 3) Found child <$($Root2ChildInfo.Id)>"
            $AttrsToAdd = ($Root1ChildInfo.Attributes | Where-Object { $_.Name -notin $Root2ChildInfo.Attributes.Name })
            $ResultPath = New-Object PathConstructor -ArgumentList $path
            $ResultPath.AddNode($Root2ChildInfo.Node.get_name())
            $ResultPath.AddAttributes(($Root2ChildInfo.Node.Attributes | Where-Object {$Config.DistinguishingAttribute -eq $_.Name}))
            if ($AttrsToAdd.Count)
            {
                Write-Verbose "$($depth) $(" " * $depth * 3) Found new attributes for child <$($Root2ChildInfo.Id)>"
                $result += @{
                    Path = $ResultPath.GetPath()
                    Object = $AttrsToAdd
                    ObjectType = [PathElementType]::Attribute
                }
            }
            $result = CompareXmlTree $Root1ChildInfo.Node $Root2ChildInfoTable[$Root1ChildInfo.Id].Node $ResultPath $result $Config ($depth + 1)
        }
        else
        {
            Write-Verbose "$($depth) $(" " * $depth * 3) Not found child $($Root1ChildInfo.Id)"
            $result += @{
                Path = $path.GetPath()
                Object = $Root1ChildInfo.Node
                ObjectId = $Root1ChildInfo.Id
                ObjectType = [PathElementType]::Node
            }
        }
    }
    return $result
}

function HumanReadableCompareXmlTreeResult($result, $detailed=$false)
{
    $HrResult = @()
    foreach ($item in $result)
    {
        if ($item.ObjectType -eq [PathElementType]::Node)
        {
            $HrResult += "Add child node [$($item.ObjectId)] to $($item.Path)"
            if ($detailed)
            {
                $StringWriter = New-Object System.IO.StringWriter
                $XmlWriter = New-Object System.Xml.XmlTextWriter -ArgumentList ($StringWriter)
                $XmlWriter.Formatting = [System.Xml.Formatting]::Indented
                $item.Object.WriteTo($XmlWriter)
                $HrResult += $StringWriter.ToString()
                $HrResult += ""
            }
        }
        elseif ($item.ObjectType -eq [PathElementType]::Attribute)
        {
            $AttrNamesStr = ($item.Object | ForEach-Object {$_.Name}) -join ", "
            $HrResult += "Add attribute(s) [$AttrNamesStr] to $($item.Path)"
            if ($detailed)
            {
                $AttrNameValueStr = "< ..."
                foreach ($attr in $item.Object)
                {
                    $AttrNameValueStr += " $($attr.Name)=`"$($attr.Value)`""
                }
                $AttrNameValueStr += " >"
                $HrResult += $AttrNameValueStr
                $HrResult += ""
            }
        }
    }
    return $HrResult
}

function UpdateXml([System.Xml.XmlDocument] $doc, [Array] $UpdateInfo, [String] $UpdaterId)
{
    $root = $doc.DocumentElement
    $OldPath = ""
    $OldObjectType = $null
    $TotalUpdateCount = 0
    $AppliedUpdateCount = 0
    foreach ($item in $UpdateInfo)
    {
        $TotalUpdateCount += 1
        $Nodes = @($root.SelectNodes($item.Path) | ForEach-Object {$_})
        if ($Nodes.Count)
        {
            if ($item.ObjectType -eq [PathElementType]::Node)
            {
                Write-Verbose "Add child node [$( $item.ObjectId )] to $( $item.Path )"
                if ($OldPath -ne $item.Path -or $OldObjectType -ne $item.ObjectType)
                {
                    $AttentionText = " Attention: The section(s) below were added automatically. TO DO: Review and remove this comment "
                    $Comment = $doc.CreateComment("$(" " * $AttentionText.Length)")
                    [Void] $Nodes[-1].AppendChild($Comment)
                    $Comment = $doc.CreateComment($AttentionText)
                    [Void] $Nodes[-1].AppendChild($Comment)
                    $Comment = $doc.CreateComment(" The section(s) were added by [$UpdaterId] ")
                    [Void] $Nodes[-1].AppendChild($Comment)
                }
                $Node = $doc.ImportNode($item.Object, $true)
                [Void] $Nodes[-1].AppendChild($Node)
            }
            elseif ($item.ObjectType -eq [PathElementType]::Attribute)
            {
                $AttrNamesStr = ($item.Object | ForEach-Object { $_.Name }) -join ", "
                Write-Verbose "Add attribute(s) [$AttrNamesStr] to $( $item.Path )"
                $AttentionText = " Attention: The attribute(s) in the section below were added automatically. TO DO: Review and remove this comment "
                $Comment = $doc.CreateComment("$(" " * $AttentionText.Length)")
                [Void] $Nodes[-1].ParentNode.InsertBefore($Comment, $Nodes[-1])
                $Comment = $doc.CreateComment($AttentionText)
                [Void] $Nodes[-1].ParentNode.InsertBefore($Comment, $Nodes[-1])
                $Comment = $doc.CreateComment(" The attribute(s) [$AttrNamesStr] were added  by [$UpdaterId] ")
                [Void] $Nodes[-1].ParentNode.InsertBefore($Comment, $Nodes[-1])
                foreach ($attr in $item.Object)
                {
                    $Attribute = $doc.ImportNode($attr, $true)
                    [Void] $Nodes[-1].Attributes.Append($Attribute)
                }
            }
            $OldPath = $item.Path
            $OldObjectType = $item.ObjectType
            $AppliedUpdateCount += 1
        }
        else
        {
        Write-Host "PATH NOT FOUND $( $item.Path )" -ForegroundColor Yellow -BackgroundColor Red
        }
    }
    return $AppliedUpdateCount, $TotalUpdateCount
}

function ConfirmYesNo([String] $Prompt, [Hashtable] $Config)
{
    if ($Config.Quiet)
    {
        return $true
    }
    else
    {
        do
        {
            $answer = Read-Host "$Prompt [Y/N]"
        }
        while ($answer -notin ("Y", "N"))
        return ($answer -eq "Y")
    }
}

Write-Verbose "Load XML #1 (Left)"
$xml1 = New-Object System.Xml.XmlDocument
$xml1.Load($Source)
$xml1Root = $xml1.DocumentElement

Write-Verbose "Load XML #2 (Right)"
$xml2 = New-Object System.Xml.XmlDocument
$xml2.Load($Destination)
$xml2Root = $xml2.DocumentElement

Write-Verbose "Load XML #3 (Right)"
$xml3 = New-Object System.Xml.XmlDocument
$xml3.Load($Destination)

Write-Verbose "Compress XML #1"
CompressXmlTree $xml1Root $Config
Write-Verbose "Compress XML #2"
CompressXmlTree $xml2Root $Config

Write-Verbose "Compare XML tree (Left -> Right)"
$result = CompareXmlTree $xml1Root $xml2Root (New-Object PathConstructor -ArgumentList ($xml2Root.get_name(), $Config)) @() $Config

if ($result.Count)
{
    if ($ReportMode)
    {
        Write-Host "The following changes need to be applied to `"$( $Destination )`":"
        HumanReadableCompareXmlTreeResult $result $ReportMode | ForEach-Object { Write-Host $_ -ForegroundColor Yellow -BackgroundColor DarkGreen }
    }
    else
    {
        if (-not $Quiet)
        {
            Write-Host "The following changes will be applied to `"$( $Destination )`":"
            HumanReadableCompareXmlTreeResult $result | ForEach-Object { Write-Host $_ -ForegroundColor Yellow -BackgroundColor DarkGreen }
        }

        if (ConfirmYesNo "Do you want to apply changes" $Config)
        {
            Write-Verbose "Update XML #3 (Left -> Right)"
            $AppliedUpdateCount, $TotalUpdateCount = UpdateXml $xml3 $result $UpdaterId

            if ($AppliedUpdateCount -gt 0)
            {
                if (-not $DontBackup)
                {
                    Write-Host "Back up `"$Destination`" to `"$Destination.$Timestamp.bak`""
                    Copy-Item $Destination "$Destination.$Timestamp.bak"
                }

                Write-Verbose "Save XML #3"
                $xml3.Save($Destination)
                Write-Host "Verified (updated) `"$Destination`""
            }
            else
            {
                Write-Host "Verified (left intact) `"$Destination`""
            }
        }
        else
        {
            Write-Host "Verified (left intact) `"$Destination`""
        }
    }
}
else
{
    Write-Host "Verified (left intact) `"$Destination`""
}
