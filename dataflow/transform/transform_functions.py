from datetime import datetime
import ast

def add_timestamp_now(element, new_field):
    now = datetime.now()
    today = now.strftime("%Y-%m-%d %H:%M:%S")
    element[new_field] = today
    return element

def add_date_now(element, new_field):
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    element[new_field] = today
    return element

def add_hardcoded_values(element, new_field, value):
    element[new_field] = value
    return element

def stringlist_to_string(element, column_name):
    string_list = element[column_name]
    list_col = ast.literal_eval(string_list)
    str_col = ','.join(str(e) for e in list_col)

    element[column_name] = str(str_col)
    return element


