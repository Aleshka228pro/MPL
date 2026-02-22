import re  # регулярные выражения - поиск и замена шаблонов в тексте

INPUT_FILE = "test.cpp"  # входной файл с c++ кодом
OUTPUT_FILE = "output.py"  # выходной файл с python кодом


# переводит код с c++ на python
def translate_cpp_to_python(lines):
    py_lines = []
    indent = 0  # уровень отступа

    functions = set()  # множество имён функций
    variables = set()  # множество имён переменных

    def add_line(line):
        py_lines.append("    " * indent + line)

    for line in lines:
        line = line.strip()

        if not line:
            continue

        if line.startswith("#include") or line.startswith("using namespace"):
            continue

        # определение функции
        match = re.match(r'int (\w+)\((.*?)\)\s*\{', line)
        if match:
            name = match.group(1)
            args = match.group(2)

            functions.add(name)

            args = re.sub(r'int ', '', args)
            arg_list = [a.strip() for a in args.split(",") if a.strip()]

            for a in arg_list:
                variables.add(a)

            if name == "main":
                add_line("if __name__ == '__main__':")
            else:
                add_line(f"def {name}({', '.join(arg_list)}):")

            indent += 1  # увеличиваем отступ внутри функции
            continue

        # объявление переменных
        if line.startswith("int "):
            vars_part = line.replace("int", "").replace(";", "")
            vars_list = [v.strip() for v in vars_part.split(",")]

            for v in vars_list:
                variables.add(v)
                add_line(f"{v} = None")
            continue

        # if
        match = re.match(r'if\s*\((.*)\)\s*\{', line)
        if match:
            add_line(f"if {match.group(1)}:")
            indent += 1  # увеличиваем отступ внутри блока if
            continue

        # else
        if line.startswith("} else"):
            indent -= 1  # закрываем предыдущий блок
            add_line("else:")
            if line.endswith("{"):
                indent += 1  # открываем блок else
            continue

        # for
        match = re.match(r'for\s*\(int (\w+) = (\d+); \1 < (\w+); \1\+\+\)', line)
        if match:
            var, start, end = match.groups()
            variables.add(var)
            add_line(f"for {var} in range({start}, {end}):")
            indent += 1  # увеличиваем отступ внутри цикла
            continue

        # cin
        if line.startswith("cin >>"):
            vars_ = [v.strip() for v in line.replace("cin >>", "").replace(";", "").split(">>")]

            for v in vars_:
                variables.add(v)

            if len(vars_) == 1:
                add_line(f"{vars_[0]} = int(input())")
            else:
                add_line(f"{', '.join(vars_)} = map(int, input().split())")
            continue

        # cout
        if line.startswith("cout <<"):
            content = line.replace("cout <<", "").replace("<< endl", "").replace(";", "")
            parts = [p.strip() for p in content.split("<<")]
            add_line(f"print({', '.join(parts)})")
            continue

        # return
        if line.startswith("return"):
            if "return 0" in line:
                continue
            add_line(line.replace(";", ""))
            continue

        if line == "}":
            indent -= 1  # уменьшаем отступ
            continue

    return py_lines, functions, variables


if __name__ == "__main__":
    print("Запуск транслятора C++ → Python...")
    print(f"")

    with open(INPUT_FILE, encoding="utf-8") as f:
        cpp_code = f.readlines()

    print(f"Прочитано строк: {len(cpp_code)}")
    print(f"")

    python_code, functions, variables = translate_cpp_to_python(cpp_code)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(python_code))

    print("Трансляция завершена")
    print(f"Результат сохранён в: {OUTPUT_FILE}")