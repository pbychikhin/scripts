
import os
import sys
from openpyxl.cell import Cell
from openpyxl.utils import get_column_letter
from os.path import basename
from datetime import datetime


def logmsg(msg):
    print(msg, file=sys.stderr)


def get_report_file_name(with_timestamp=True):
    file_name = basename(sys.argv[0]).rsplit(".", 1)
    if with_timestamp:
        file_ext = [datetime.now().strftime("%y%m%d%H%M%S"), "xlsx"]
    else:
        file_ext = ["xlsx"]
    if len(file_name) > 1 and file_name[-1].strip().lower() == "py":
        file_name[-1:] = file_ext
    else:
        file_name += file_ext
    if with_timestamp:
        file_name[-3] = "_".join(file_name[-3:-1])
        del file_name[-2]
    return  ".".join(file_name)


def get_report_dir_name():
    dir_name = basename(sys.argv[0]).rsplit(".", 1)
    if len(dir_name) > 1 and dir_name[-1].strip().lower() == "py":
        dir_name[-1] = "out"
    else:
        dir_name.append("out")
    return  "_".join(dir_name)


def adjust_col_width(sheet, row_i, header):
    for col_i in range(len(header)):
        value_len = len(str(sheet[row_i][col_i].value))
        if sheet.column_dimensions[get_column_letter(col_i + 1)].width <= value_len:
            sheet.column_dimensions[get_column_letter(col_i + 1)].width = (value_len + 1)


class CellProperties:
    def __init__(self, base_cell: Cell):
        self.base_font = base_cell.font