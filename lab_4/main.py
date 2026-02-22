import time  # пауза между запросами
import requests  # простые HTTP-запросы - отправка HTTP-запросов к сайту
import pandas as pd  # работа с Excel
from bs4 import BeautifulSoup  # парсинг HTML, поиск элементов по селекторам
from pathlib import Path  # проверка и удаление файлов
# openpyxl==3.1.2 - запись xlsx файлов

INPUT_FILE = "Links.xlsx"  # файл со ссылками
OUTPUT_FILE = "output.xlsx"  # файл для результатов
BASE = "https://www.vesselfinder.com"  # сайт (убраны лишние пробелы)
HEADERS = {  # иначе не пускает и выдает ошибку
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}

DELAY = 1  # секунды между запросами (чтобы не заблокировали)
NUM_TO_PROCESS = 50  # количество ссылок для обработки (0 = все)


# читает ссылки из первого столбца excel файла пропуская заголовок
def get_links_from_excel(filename):
    df = pd.read_excel(filename, header=0, usecols=[0])
    links = df.iloc[:, 0].dropna().astype(str).tolist()

    result = []
    for link in links:
        link = link.strip()
        if link and link.lower() != "ссылка":
            result.append(link)

    return result

def get_html(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    return response.text


# проверяет, найдено ли ровно одно судно
def check_single_vessel(html):
    soup = BeautifulSoup(html, "lxml")

    page_text = soup.get_text()

    if "1 судно" not in page_text and "1 судов" not in page_text:
        print("Найдено не одно судно или судна отсутствуют")
        return None

    print("Найдено одно судно")

    # ищем ссылку на страницу судна
    link_tag = soup.select_one("td a[href^='/ru/vessels/details/']")

    if link_tag and link_tag.get("href"):
        relative_link = link_tag["href"]
        full_url = BASE + relative_link
        print(f"Ссылка на судно: {full_url}")
        return full_url

    print("Не удалось найти ссылку")
    return None


# извлекает данные судна: название IMO MMSI тип
def parse_vessel_info(html):
    soup = BeautifulSoup(html, "lxml")

    vessel = {
        "Название": "",
        "IMO": "",
        "MMSI": "",
        "Тип судна": ""
    }

    title_elem = soup.select_one(".title")
    if title_elem:
        vessel["Название"] = title_elem.get_text(strip=True)

    table = soup.select_one("table.aparams")

    if table:
        rows = table.select("tr")

        for row in rows:
            header_td = row.select_one("td.n3")  # заголовок параметра
            value_td = row.select_one("td.v3")  # значение параметра

            if not header_td or not value_td:
                continue

            header = header_td.get_text(strip=True)
            value = value_td.get_text(strip=True)

            if "MMSI" in header:
                if "IMO" in header and "/" in value:
                    parts = value.split("/")
                    if len(parts) >= 2:
                        vessel["IMO"] = parts[0].strip()
                        vessel["MMSI"] = parts[1].strip()
                else:
                    vessel["MMSI"] = value

            if "AIS тип" in header or "Тип судна" in header:
                vessel["Тип судна"] = value

    if vessel["Название"]:
        print(f" {vessel['Название']} | IMO: {vessel['IMO']} | MMSI: {vessel['MMSI']} | Тип: {vessel['Тип судна']}")

    return vessel


# создаёт новый excel файл с заголовками
def init_excel(filename, headers):
    if Path(filename).exists():
        Path(filename).unlink()

    df = pd.DataFrame(columns=headers)
    df.to_excel(filename, index=False, engine="openpyxl")

    print(f"Файл {filename} создан")


# добавляет строку с данными в excel файл
def append_to_excel(filename, data):
    df_existing = pd.read_excel(filename)

    new_row = pd.DataFrame([data])

    df_updated = pd.concat([df_existing, new_row], ignore_index=True)

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df_updated.to_excel(writer, index=False)


def main():
    links = get_links_from_excel(INPUT_FILE)

    print(f"Найдено ссылок: {len(links)}")

    headers = ["Название", "IMO", "MMSI", "Тип судна"]
    init_excel(OUTPUT_FILE, headers)

    if NUM_TO_PROCESS > 0:
        limit = NUM_TO_PROCESS
    else:
        limit = len(links)

    cnt = 0

    for idx in range(limit):
        url = links[idx]

        print(f"\n[{idx + 1}/{limit}] Обработка: {url}")

        time.sleep(DELAY)

        html = get_html(url)

        vessel_url = check_single_vessel(html)
        if not vessel_url:
            continue

        vessel_html = get_html(vessel_url)

        vessel_data = parse_vessel_info(vessel_html)

        if not vessel_data["Название"]:
            print("Не удалось получить данные судна")
            continue

        append_to_excel(OUTPUT_FILE, vessel_data)
        cnt += 1
        print(f"Добавлено в {OUTPUT_FILE}")

    print(f"\nЗаписано: {cnt}/{limit}")
    print(f"Результаты сохранены в: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()