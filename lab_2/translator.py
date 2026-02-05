import re

def translate_cpp_to_python(lines):
    py_lines = []
    indent = 0

    functions = set()
    variables = set()

    def add_line(line):
        py_lines.append("    " * indent + line)

    for line in lines:
        line = line.strip()

        if not line:
            continue

        # ignore includes
        if line.startswith("#include") or line.startswith("using namespace"):
            continue

        # function definition
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

            indent += 1
            continue

        # variable declaration
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
            indent += 1
            continue

        # else
        if line.startswith("} else"):
            indent -= 1
            add_line("else:")
            if line.endswith("{"):
                indent += 1
            continue

        # for
        match = re.match(r'for\s*\(int (\w+) = (\d+); \1 < (\w+); \1\+\+\)', line)
        if match:
            var, start, end = match.groups()
            variables.add(var)
            add_line(f"for {var} in range({start}, {end}):")
            indent += 1
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

        # block end
        if line == "}":
            indent -= 1
            continue

    return py_lines, functions, variables


# ===== MAIN =====
if __name__ == "__main__":
    with open("test.cpp", encoding="utf-8") as f:
        cpp_code = f.readlines()

    python_code, functions, variables = translate_cpp_to_python(cpp_code)

    with open("output.py", "w", encoding="utf-8") as f:
        f.write("\n".join(python_code))

    print("Трансляция завершена")
    print("Функции:", functions)
    print("Переменные:", variables)
