import time

from dateutil import parser


def powerful_format_date(string_date):
    """
    格式化时间
    """
    try:
        format_date = parser.parse(string_date)
    except:
        try:
            format_date = parser.parse(string_date, fuzzy=True)
        except:
            return time.strftime('%Y-%m-%d %H:%M:%S')
    return str(format_date)

# print(str(powerful_format_date('22/2/2021 2:42')))
# print(str(powerful_format_date('23-2-2021 | 21:20')))
