from datetime import datetime

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



