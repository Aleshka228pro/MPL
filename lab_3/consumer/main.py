import json # работа с JSON данными - парсинг и сериализация сообщений
import re # регулярные выражения - валидация названий таблиц и колонок
from confluent_kafka import Consumer, KafkaException # Consumer - получение сообщений из топика, KafkaException - обработка ошибок брокера
import psycopg2 # работа с postgreSQL - подключене к БД и выполнение SQL запросов
from typing import Dict, List, Any # # модуль подсказок типов: Dict (словарь), List (список), Any (любой тип) для описания структур данных


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092', # адрес Kafka брокера
    'group.id': 'etl_consumer_group', # идентификатор группы потребителей (для балансировки нагрузки)
    'auto.offset.reset': 'earliest', # с какого места читать, если нет сохраненного смещения ('earliest' = с самого начала)
    'enable.auto.commit': True, # автоматически подтверждать прочитанные сообщения
}

KAFKA_TOPIC = 'etl_data'

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'db',
    'user': 'mpl_3',
    'password': 'zmn'
}


# валидация
def validate_table_name(table_name: str) -> tuple:
    if not table_name or not table_name.strip():
        return False, "Название таблицы не может быть пустым"
    if len(table_name) > 50:
        return False, "Название слишком длинное"
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return False, "Только буквы, цифры и _"
    return True, ""


# создает подключение к бд
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# создает таблицу
def create_table_if_not_exists(table_name: str, columns: Dict[str, str]) -> bool:
    safe_table_name = table_name.replace('"', '""')

    columns_def = []
    for col_name, col_type in columns.items():
        safe_col_name = col_name.replace('"', '""')
        columns_def.append(f'"{safe_col_name}" TEXT')

    create_query = f'''
        CREATE TABLE IF NOT EXISTS "{safe_table_name}" (
            {', '.join(columns_def)}
        )
    '''

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_query)
        conn.commit()
        print(f"Таблица '{table_name}' создана")
        return True
    except Exception as e:
        print(f"Ошибка создания таблицы: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# вставляет данные в таблицу
def insert_data(table_name: str, columns: Dict[str, str], data: List[Dict[str, Any]]) -> bool:
    if not data:
        print("Нет данных для вставки")
        return True

    safe_table_name = table_name.replace('"', '""')
    column_names = list(columns.keys())

    placeholders = ', '.join(['%s'] * len(column_names))
    columns_str = ', '.join(['"' + col.replace('"', '""') + '"' for col in column_names])

    insert_query = f'''
        INSERT INTO "{safe_table_name}" ({columns_str})
        VALUES ({placeholders})
    '''

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for row in data:
            values = [str(row.get(col, '')) for col in column_names]
            cursor.execute(insert_query, values)

        conn.commit()
        print(f"Вставлено {len(data)} строк в '{table_name}'")
        return True

    except Exception as e:
        print(f"Ошибка вставки данных: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# обработка сообщения
def process_message(message_value: bytes) -> bool:
    try:
        message = json.loads(message_value.decode('utf-8'))

        table_name = message.get('table_name')
        columns = message.get('columns', {})
        data = message.get('data', [])

        print(f"   Получено сообщение для таблицы '{table_name}'")
        print(f"   Столбцы: {list(columns.keys())}")
        print(f"   Строк: {len(data)}")

        # валидация
        if not table_name:
            print("Нет названия таблицы")
            return False

        is_valid, error = validate_table_name(table_name)
        if not is_valid:
            print(f"Ошибка таблицы: {error}")
            return False

        if not columns:
            print("Нет структуры колонок")
            return False

        # создание таблицы
        if not create_table_if_not_exists(table_name, columns):
            return False

        # вставка данных
        if not insert_data(table_name, columns, data):
            return False

        print("Сообщение успешно обработано")
        return True

    except json.JSONDecodeError as e:
        print(f"Ошибка парсинга JSON: {e}")
        return False
    except Exception as e:
        print(f"Ошибка: {e}")
        return False


def start_consumer():
    print(f"   Kafka: {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

    consumer = None
    try:
        consumer = Consumer(KAFKA_CONFIG)
        consumer.subscribe([KAFKA_TOPIC])

        print(f"Consumer подписан на топик '{KAFKA_TOPIC}'")
        print("Ожидание сообщений... (нажмите Ctrl+C для остановки)")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Ошибка Kafka: {msg.error()}")
                continue

            message_value = msg.value()
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()

            print(f"Сообщение из {topic}[{partition}]@{offset}")

            success = process_message(message_value)

            if success:
                print("Обработано успешно")
            else:
                print("Обработано с ошибками")

    except KeyboardInterrupt:
        print("\nОстановка Consumer")
    except KafkaException as e:
        print(f"Kafka ошибка: {e}")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumer закрыт")


if __name__ == "__main__":

    start_consumer()
