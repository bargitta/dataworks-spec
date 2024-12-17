import ast
import os
import sys
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

formatted_mapping = {}
engine_mapping = {}
comment_annotation = '-- '
sql_names = ['sql']


def in_sql_names(name):
    if name in sql_names:
        return True
    else:
        return False


def replace_formatted_name(source):
    try:
        return formatted_mapping[source]
    except KeyError:
        return source


def replace_engine_name(source):
    try:
        return engine_mapping[source]
    except KeyError:
        return source


def extract_sql(file):
    with open(file, 'r', encoding='utf-8') as f:
        node = ast.parse(f.read(), file)

    values = []
    for item in node.body:
        if isinstance(item, ast.If) and isinstance(item.test, ast.Compare):
            if (isinstance(item.test.left, ast.Name) and item.test.left.id == '__name__' and
                    any(isinstance(op, ast.Eq) for op in item.test.ops) and
                    any(isinstance(comp, ast.Str) and comp.s == '__main__' for comp in item.test.comparators)):
                for main_item in item.body:
                    if isinstance(main_item, ast.Assign):
                        for target in main_item.targets:
                            if isinstance(target, ast.Name):
                                if in_sql_names(target.id):
                                    if isinstance(main_item.value, ast.JoinedStr):
                                        for v in main_item.value.values:
                                            if isinstance(v, ast.Constant):
                                                values.append(v.value)
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
    sql = extract_sql(filename)
    comment = read_python_as_comment(filename)
    data = ''.join(sql)
    data = comment + '\n\n' + data
    print(data)
