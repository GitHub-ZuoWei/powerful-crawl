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
            return ''
    return str(format_date)


print(str(powerful_format_date('2021-02-02 14:14:09+08:00')))
