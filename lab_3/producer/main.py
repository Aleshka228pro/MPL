import streamlit as st  # создание веб-интерфейса - интерактивные формы и кнопки
import json  # работа с JSON данными - парсинг и сериализация сообщений
import csv  # работа с CSV файлами - чтение и парсинг табличных данных
import io  # работа с потоками ввода-вывода - stringIO для обработки CSV в памяти
import re  # регулярные выражения - валидация названий таблиц и колонок
from confluent_kafka import Producer  # клиент Apache Kafka - отправка сообщений в топик



KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
}

KAFKA_TOPIC = 'etl_data'


# валидация
# проверка названия таблицы
def validate_table_name(table_name: str) -> tuple:
    if not table_name or not table_name.strip():
        return False, "Название таблицы не может быть пустым"
    if len(table_name) > 50:
        return False, "Название слишком длинное (макс. 50 символов)"
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return False, "Только буквы, цифры и _"
    return True, ""

# проверка названия столбцов
def validate_columns(columns_list: list) -> tuple:
    if not columns_list:
        return False, "Столбцы не могут быть пустыми"
    for col in columns_list:
        if not col or not col.strip():
            return False, "Найдены пустые названия столбцов"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col.strip()):
            return False, f"Неверное название столбца: '{col}'"
    return True, ""



# отправка в Kafka
def send_to_kafka(message: dict) -> bool:
    producer = Producer(KAFKA_CONFIG)
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(message).encode('utf-8'),
        )
        producer.flush()
        return True
    except Exception as e:
        st.error(f"Ошибка отправки в Kafka: {e}")
        return False



# парсит CSV файл и возвращает (columns, data, error)
def parse_csv_file(file) -> tuple:
    try:
        content = file.read().decode('utf-8')
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)

        if len(rows) < 2:
            return None, None, "CSV файл должен содержать заголовок и хотя бы одну строку данных"

        columns = [col.strip() for col in rows[0]]
        data = []

        for i, row in enumerate(rows[1:], start=2):
            if len(row) != len(columns):
                return None, None, f"Строка {i}: количество значений не совпадает с заголовком"
            row_dict = {columns[j]: row[j].strip() for j in range(len(columns))}
            data.append(row_dict)

        return columns, data, None

    except Exception as e:
        return None, None, f"Ошибка чтения CSV: {e}"

# парсит JSON файл и возвращает (columns, data, error)
def parse_json_file(file) -> tuple:
    try:
        content = file.read().decode('utf-8')
        data = json.loads(content)

        if not isinstance(data, list):
            return None, None, "JSON должен содержать массив объектов"

        if len(data) == 0:
            return None, None, "JSON массив пуст"

        columns = list(data[0].keys())

        for i, obj in enumerate(data):
            if set(obj.keys()) != set(columns):
                return None, None, f"Объект {i}: несовпадающие ключи"

        # преобразуем все значения в строки
        for obj in data:
            for key in obj:
                obj[key] = str(obj[key]) if obj[key] is not None else ''

        return columns, data, None

    except json.JSONDecodeError as e:
        return None, None, f"Ошибка парсинга JSON: {e}"
    except Exception as e:
        return None, None, f"Ошибка чтения JSON: {e}"


# streamlit интерфейс
st.set_page_config(page_title="ETL Producer", layout="wide")
st.title("ETL")

input_method = st.radio(
    "Способ ввода:",
    ["Ручной ввод", "Загрузить файл (CSV/JSON)"]
)

table_name = ""
columns_list = []
data_rows = []


if input_method == "Ручной ввод":
    table_name = st.text_input("Название таблицы", placeholder="например: users")

    columns_input = st.text_input(
        "Столбцы (через запятую)",
        placeholder="id, name, age"
    )

    data_input = st.text_area(
        "Данные (каждая строка — запись, значения через запятую)",
        placeholder="""1, Igor, 22
2, Oleg, 21
3, Gloeb, 33""",
        height=150
    )

    # парсинг ручного ввода
    if columns_input:
        columns_list = [c.strip() for c in columns_input.split(',')]

    if data_input:
        lines = data_input.strip().split('\n')
        for line in lines:
            if not line.strip():
                continue
            values = [v.strip() for v in line.split(',')]
            if len(values) == len(columns_list):
                row_dict = {columns_list[j]: values[j] for j in range(len(columns_list))}
                data_rows.append(row_dict)


# загрузка файла
elif input_method == "Загрузить файл (CSV/JSON)":

    uploaded_file = st.file_uploader(
        "Выберите файл CSV или JSON",
        type=['csv', 'json']
    )

    if uploaded_file:
        file_type = uploaded_file.name.split('.')[-1].lower()

        if file_type == 'csv':
            columns_list, data_rows, error = parse_csv_file(uploaded_file)
        elif file_type == 'json':
            columns_list, data_rows, error = parse_json_file(uploaded_file)

        if error:
            st.error(f"{error}")
        elif columns_list and data_rows:
            st.success(f"Загружено: {len(data_rows)} строк")

            # автозаполнение названия таблицы из имени файла
            if not table_name:
                table_name = st.text_input(
                    "Название таблицы",
                    value=uploaded_file.name.split('.')[0]
                )


# отправка
if st.button("Отправить", type="primary"):
    if not table_name:
        st.error("Введите название таблицы!")
    elif not columns_list:
        st.error("Нет столбцов!")
    elif not data_rows:
        st.error("Нет данных!")
    else:
        is_valid, error = validate_table_name(table_name)
        if not is_valid:
            st.error(f"{error}")
        else:
            is_valid, error = validate_columns(columns_list)
            if not is_valid:
                st.error(f"{error}")
            else:
                columns_dict = {col: 'varchar' for col in columns_list}

                message = {
                    "table_name": table_name,
                    "columns": columns_dict,
                    "data": data_rows
                }

                if send_to_kafka(message):

                    st.success("Отправлено в Kafka!")
