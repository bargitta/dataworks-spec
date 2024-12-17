import ast
import os
import re
import sys
import warnings

import json

warnings.filterwarnings('ignore')

formatted_mapping = {}
engine_mapping = {
    'da': 'xingye_da',
    'dws_rec': 'dws_reco',
    'ug': 'xingye_ug',
    'risk_control': 'mn_risk_control',
    'ad': 'xingye_ad'
}
comment_annotation = '-- '
sql_names = ['sql']
parameters = []


def in_sql_names(name):
    if name in sql_names:
        return True
    else:
        return False


def replace_formatted_name(param):
    if param in formatted_mapping:
        param = formatted_mapping[param]
    param = param.lower()
    parameters.append(param + '=$[yyyymmdd-1]')
    return param


def replace_engine_name(source):
    try:
        for key, value in engine_mapping.items():
            if key in source:
                regex = r"([\S]*)(\s+){}\.([^ ]+)\s+".format(key)
                replacement = r"\1 \2{}.\3 ".format(value)
                source = re.sub(regex, replacement, source)
        return source
    except KeyError:
        return source


"""
  if 'INSERT OVERWRITE' without 'TABLE" key word, add 'TABLE'
"""

table_regex = r"(?i)(INSERT)\s+(OVERWRITE)\s+(?!table\b)([^ ]+)\s+"
table_replacement = r"\1 \2 TABLE \3 "  # 使用 \1,\2 和 \3


def add_table_word(source):
    return re.sub(table_regex, table_replacement, source)


def handle_source_replacement(source):
    value = replace_engine_name(source)
    return add_table_word(value)


def extract_sql(file):
    file_exists = os.path.exists(file)
    if not file_exists:
        print(f'file {file} not exists')

    with open(file, 'r', encoding='utf-8') as f:
        node = ast.parse(f.read(), file)

    values = []
    for item in node.body:
        if isinstance(item, ast.If) and isinstance(item.test, ast.Compare):
            if (isinstance(item.test.left, ast.Name) and item.test.left.id == '__name__' and
                    any(isinstance(op, ast.Eq) for op in item.test.ops) and
                    any(isinstance(comp, ast.Constant) and comp.value == '__main__' for comp in item.test.comparators)):
                for main_item in item.body:
                    if isinstance(main_item, ast.Assign):
                        for target in main_item.targets:
                            if isinstance(target, ast.Name):
                                if in_sql_names(target.id):
                                    if isinstance(main_item.value, ast.JoinedStr):
                                        for v in main_item.value.values:
                                            if isinstance(v, ast.Constant):
                                                replaced = handle_source_replacement(v.value)
                                                values.append(replaced)
                                            elif isinstance(v, ast.FormattedValue):
                                                if isinstance(v.value, ast.Name):
                                                    values.append("${" + replace_formatted_name(v.value.id) + "}")
    return values


def parse_python(file):
    if os.path.isdir(file):
        for name in os.listdir(file):
            try:
                file_path = file + os.sep + name
                print(f'parsing python file {file_path}')
                sql = extract_sql(file_path)
                comment = read_python_as_comment(file_path)
                data = ''.join(sql)
                data = comment + '\n\n' + data
                print(f'parsed python file {file_path}')
                print(f'name {name} : \n sql {data}')
            except Exception as e:
                print(e)


def read_python_as_comment(file):
    lines = []
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            line = comment_annotation + line
            lines.append(line)
        return ''.join(lines)


def test_read_python():
    parse_python("sources")


if __name__ == '__main__':
    filename = sys.argv[1]
    try:
        sql = extract_sql(filename)
        comment = read_python_as_comment(filename)
        data = ''.join(sql)
        data = comment + '\n\n' + 'set odps.sql.hive.compatible=true;' + '\n\n' + data
        print(json.dumps(parameters))
        print(data)
    except FileNotFoundError as e:
        print(f'file {filename} not exists')
        print(e)
        exit(1)
