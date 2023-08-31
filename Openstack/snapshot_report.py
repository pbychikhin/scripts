
from volume_report import *

if __name__ == "__main__":
    logmsg("Auth")
    auth = OsAuth()

    logmsg("Get projects")
    projects = os_api_get_projects(auth, auth.identity_url)

    logmsg("Get Volumes URL")
    volumes_url = os_api_get_service_url(auth, "volumev3")

    logmsg("Get volumes")
    volumes = OsVolumes(auth, volumes_url)

    logmsg("Prepare report")
    report_file = get_report_file_name()
    report_dir = get_report_dir_name()
    report_context = {
        "base_font": None,
        "header_font": None,
        "base_fill": None,
        "medium_fill": None,
        "high_fill": None,
        "sheets": dict()
    }
    header = ("id", "name", "size", "status", "children (direct/indirect)", "age_since_create", "age_since_update")
    wb = Workbook()
    ws = None
    for row in volumes.get_snapshots():
        project = projects.get(row["os-extended-snapshot-attributes:project_id"], {"name": "__nonexistent__"})["name"]
        if not report_context["sheets"]:
            ws = wb.active
            ws.title = project
        if project not in wb.sheetnames:
            sheetnames = wb.sheetnames
            sheetnames.append(project)
            sheetnames.sort(key=str.lower)
            wb.create_sheet(project, sheetnames.index(project))
        ws = wb[project]
        if ws.title not in report_context["sheets"]:
            report_context["sheets"][ws.title] = dict()
            ws.append(header)
            report_context["sheets"][ws.title]["row_count"] = 1
            adjust_col_width(ws, report_context["sheets"][ws.title]["row_count"], header)
            if not report_context["base_font"]:
                report_context["base_font"] = copy(ws[1][0].font)
                report_context["header_font"] = copy(report_context["base_font"])
                report_context["header_font"].bold = True
                report_context["base_fill"] = copy(ws[1][0].fill)
                report_context["medium_fill"] = copy(report_context["base_fill"])
                report_context["high_fill"] = copy(report_context["base_fill"])
                report_context["medium_fill"].patternType = "solid"
                report_context["medium_fill"].fgColor = "00FFFF00"
                report_context["high_fill"].patternType = "solid"
                report_context["high_fill"].fgColor = "00FF0000"
            for cell in ws[report_context["sheets"][ws.title]["row_count"]]:
                cell.font = report_context["header_font"]
        row_data = {header.index(k) + 1: v for k, v in row.items() if k in ("id", "name", "size", "status")}
        row_data[header.index("children (direct/indirect)") + 1] = "\n".join("{} ({} in {})".format(x["id"], x["status"], projects.get(x["os-vol-tenant-attr:tenant_id"], {"name": "__nonexistent__"})["name"]) for x in volumes.get_children_by_snap_id(row["id"]))
        row_data[header.index("age_since_create") + 1] = os_timestamp_age(row["created_at"])
        row_data[header.index("age_since_update") + 1] = os_timestamp_age(row["updated_at"] or row["created_at"])
        ws.append(row_data)
        report_context["sheets"][ws.title]["row_count"] += 1
        adjust_col_width(ws, report_context["sheets"][ws.title]["row_count"], header)

    if not exists(report_dir):
        logmsg("Create report dir")
        os.mkdir(report_dir)
    else:
        logmsg("Clean the report dir up")
        for i in os.scandir(report_dir):
            if i.name.lower().endswith(".xlsx"):
                i_path = path_join(report_dir, i.name)
                logmsg("  Remove {}".format(i_path))
                os.remove(i_path)
    report_file_path = path_join(report_dir, report_file)
    logmsg("Write general report {}".format(report_file_path))
    wb.save(report_file_path)
    logmsg("Write per-project reports")
    orig_sheets = wb._sheets    # manipulate protected member for efficiency
    for sheetname in wb.sheetnames:
        wb._sheets = [sheet for sheet in orig_sheets if sheet.title == sheetname]
        report_file_path = path_join(report_dir, "project_{}.xlsx".format(sheetname))
        logmsg("  Write {}".format(report_file_path))
        wb.save(report_file_path)
