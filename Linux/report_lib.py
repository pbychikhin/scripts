
import os
import sys
from copy import copy
from openpyxl.cell import Cell
# from openpyxl.styles.numbers import FORMAT_NUMBER_00
from os.path import basename
from datetime import datetime
from os.path import exists as path_exists
from os.path import join as path_join


__all__ = ["logmsg",
           "get_report_file_name",
           "get_report_dir_name",
           "prepare_report_file",
           "adjust_col_width",
           "CellProperties"]


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


def prepare_report_file(with_timestamp=True):
    report_dir = get_report_dir_name()
    report_file = get_report_file_name(with_timestamp)
    if not path_exists(report_dir):
        logmsg("Create report dir")
        os.mkdir(report_dir)
    return path_join(report_dir, report_file)


def adjust_col_width(sheet, row_i):
    for cell in sheet[row_i]:
        if isinstance(cell.value, float):
            value_len = len(str(round(cell.value, 2)))
        else:
            value_len = len(str(cell.value))
        if sheet.column_dimensions[cell.column_letter].width <= value_len:
            sheet.column_dimensions[cell.column_letter].width = (value_len + 1)


class CellProperties:
    def __init__(self, base_cell: Cell):
        self.base_font = copy(base_cell.font)
        self.header_font = copy(self.base_font)
        self.header_font.bold = True
        self.base_fill = copy(base_cell.fill)
        self.medium_fill = copy(self.base_fill)
        self.high_fill = copy(self.base_fill)
        self.medium_fill.patternType = "solid"
        self.medium_fill.fgColor = "00FFFF00"
        self.high_fill.patternType = "solid"
        self.high_fill.fgColor = "00FF0000"
        self.float_number = copy(base_cell.number_format)
        self.float_number = "0.##"

    def BaseFont(self, cell: Cell):
        cell.font = self.base_font

    def HeaderFont(self, cell: Cell):
        cell.font = self.header_font

    def BaseFill(self, cell: Cell):
        cell.fill = self.base_fill

    def MediumFill(self, cell: Cell):
        cell.fill = self.medium_fill

    def HighFill(self, cell: Cell):
        cell.fill = self.high_fill

    def FloatNumber(self, cell: Cell):
        cell.number_format = self.float_number
