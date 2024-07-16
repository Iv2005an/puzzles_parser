import asyncio
from datetime import datetime
from aiohttp import ClientSession, TCPConnector, ClientTimeout, ClientConnectionError
import aiofiles
from bs4 import BeautifulSoup, Tag
from pathlib import Path
import re
import requests

from puzzles_parser.models import Puzzle


class PuzzlesParser:
    def __init__(self) -> None:
        self.__HOST_URL = "https://hobby-puzzle.ru/"
        self.__START_PAGE_URL = "proizvoditeli.html"
        self.__SAVE_DIRECTORY_PATH = Path.joinpath(
            Path.home(), "PuzzlesParser"
        ).as_posix()
        self.__SAVE_IMAGES_DIRECTORY_PATH = Path.joinpath(
            Path(self.__SAVE_DIRECTORY_PATH), "images"
        ).as_posix()
        Path(self.__SAVE_IMAGES_DIRECTORY_PATH).mkdir(parents=True, exist_ok=True)
        self.__CACHE_DIRECTORY_PATH = Path.joinpath(
            Path(self.__SAVE_DIRECTORY_PATH), "cache"
        )
        self.__CACHE_PAGES_DIRECTORY_PATH = Path.joinpath(
            Path(self.__CACHE_DIRECTORY_PATH), "pages"
        )
        Path(self.__CACHE_PAGES_DIRECTORY_PATH).mkdir(parents=True, exist_ok=True)
        self.__CACHE_PUZZLES_DIRECTORY_PATH = Path.joinpath(
            Path(self.__CACHE_DIRECTORY_PATH), "puzzles"
        )
        Path(self.__CACHE_PUZZLES_DIRECTORY_PATH).mkdir(parents=True, exist_ok=True)
        self.__puzzles_count: int
        self.__pages_count: int
        self.__get_count_data()
        self.__completed_puzzles_count: int
        self.__completed_pages_count: int
        self.__puzzles: list[Puzzle] = []
        print(f"Страниц подлежащих обработке: {self.__pages_count}")
        print(f"Пазлов подлежащих обработке: {self.__puzzles_count}")

    def __get_count_data(self):
        page = BeautifulSoup(self.__get_start_page(), "lxml")
        navigation_bar = page.find("div", class_="navigation")
        if type(navigation_bar) is Tag:
            positions = navigation_bar.find_all("span", class_="bold")
            self.__puzzles_count = int(positions[2].text)
            self.__pages_count = self.__puzzles_count // int(positions[1].text) + 1

    def __get_start_page(self) -> str:
        page_file_path = Path.joinpath(
            Path(self.__CACHE_PAGES_DIRECTORY_PATH), self.__get_page_filename(1)
        )
        if Path.is_file(page_file_path):
            with open(page_file_path) as page_file:
                return page_file.read()
        response = requests.get(self.__HOST_URL + self.__START_PAGE_URL)
        response.raise_for_status()
        with open(page_file_path, "w") as page_file:
            page_file.write(response.text)
        return response.text

    def __get_page_filename(self, i) -> str:
        name, extension = self.__START_PAGE_URL.split(".")
        return f"{name}_{i}.{extension}"

    async def __create_open_pages_tasks(self):
        open_pages_tasks = set()
        async with ClientSession(
            connector=TCPConnector(verify_ssl=False, limit=10),
            timeout=ClientTimeout(),
            trust_env=True,
            raise_for_status=True,
        ) as session:
            for i in range(1, self.__pages_count + 1):
                open_pages_tasks.add(
                    asyncio.create_task(self.__create_open_puzzle_tasks(session, i))
                )
            await asyncio.gather(*open_pages_tasks)

    async def __create_open_puzzle_tasks(self, session: ClientSession, i: int):
        page_file_path = Path.joinpath(
            Path(self.__CACHE_PAGES_DIRECTORY_PATH), self.__get_page_filename(i)
        )
        if Path.is_file(page_file_path):
            while True:
                try:
                    async with aiofiles.open(page_file_path) as page_file:
                        page_text = await page_file.read()
                    break
                except OSError:
                    await asyncio.sleep(1)
            print(f"СТРАНИЦЫ: Получена {i} из кеша")
        else:
            print(f"СТРАНИЦЫ: Получение {i}")
            while True:
                try:
                    async with session.get(
                        f"{self.__HOST_URL}{self.__START_PAGE_URL}?page={i}"
                    ) as response:
                        page_text = await response.text()
                    break
                except ClientConnectionError:
                    await asyncio.sleep(1)
            while True:
                try:
                    async with aiofiles.open(page_file_path, "w") as page_file:
                        await page_file.write(page_text)
                    break
                except OSError:
                    await asyncio.sleep(1)
            print(f"СТРАНИЦЫ: Получена {i}")
        page = BeautifulSoup(page_text, "lxml")
        puzzles_urls: list[str] = []
        for puzzle_block in page.find_all("div", class_="card-body"):
            puzzle_url = puzzle_block.find("a").get("href")
            if puzzle_url.find("3d") == -1:
                puzzles_urls.append(puzzle_url)
            else:
                self.__puzzles_count -= 1
        parse_puzzles_tasks = set()
        for puzzle_url in puzzles_urls:
            parse_puzzles_tasks.add(
                asyncio.create_task(self.__parse_puzzle(session, puzzle_url))
            )
        await asyncio.gather(*parse_puzzles_tasks)
        self.__completed_pages_count += 1
        print(
            f"СТРАНИЦЫ: Обработана {i}, осталось {self.__pages_count-self.__completed_pages_count}"
        )

    async def __parse_puzzle(self, session: ClientSession, url: str):
        puzzle_file_name = url.split("/")[-1]
        puzzle_simple_file_name = puzzle_file_name.split(".")[0]
        puzzle_file_path = Path.joinpath(
            Path(self.__CACHE_PUZZLES_DIRECTORY_PATH), puzzle_file_name
        )
        if Path.is_file(puzzle_file_path):
            while True:
                try:
                    async with aiofiles.open(puzzle_file_path) as puzzle_file:
                        puzzle_page_text = await puzzle_file.read()
                    break
                except OSError:
                    await asyncio.sleep(1)
            print(f"ПАЗЛЫ: Получен из кеша {puzzle_simple_file_name}")
        else:
            print(f"ПАЗЛЫ: Получение {puzzle_simple_file_name}")
            while True:
                try:
                    async with session.get(url) as response:
                        puzzle_page_text = await response.text()
                    break
                except ClientConnectionError:
                    await asyncio.sleep(1)
            while True:
                try:
                    async with aiofiles.open(puzzle_file_path, "w") as puzzle_file:
                        await puzzle_file.write(puzzle_page_text)
                    break
                except OSError:
                    await asyncio.sleep(1)
            print(f"ПАЗЛЫ: Получен {puzzle_simple_file_name}")
        page = BeautifulSoup(puzzle_page_text, "lxml")
        puzzle_card = page.find("div", class_="card-body")
        if type(puzzle_card) is Tag:
            image_block = puzzle_card.find("a")
            article_block = puzzle_card.select_one(
                "div.col-12.col-md-5.font-weight-bold"
            )
            title_block = puzzle_card.find("h1")
            properties_block = puzzle_card.select_one(
                "div.description.extra_fields.text-muted"
            )
            if (
                type(image_block) is Tag
                and type(article_block) is Tag
                and type(title_block) is Tag
                and type(properties_block) is Tag
            ):
                if title_block.text.lower().find("3d") != -1:
                    self.__puzzles_count -= 1
                    return
                try:
                    article_number = int(re.findall(r"\d+", article_block.text)[0])
                except IndexError:
                    article_number = 0
                title_match = list(re.findall(r"\"(.*)\"", title_block.text))
                if len(title_match) > 0:
                    title = title_match[0]
                else:
                    title_match = list(re.findall(r": (.*) \d", title_block.text))
                    if len(title_match) > 0:
                        title = title_match[0]
                    else:
                        title_match = list(
                            re.findall(r"Пазл (.+) \d", title_block.text)
                        )
                        if len(title_match) > 0:
                            title = title_match[0]
                        else:
                            title_match = list(
                                re.findall(r"деталей (.+)", title_block.text)
                            )
                            if len(title_match) > 0:
                                title = title_match[0]
                            else:
                                title = title_block.text

                properties: dict[str, str] = {}
                properties_names = properties_block.select(
                    "div.col-12.col-md-5.font-weight-bold"
                )
                for property_name in properties_names:
                    property_value_block = property_name.find_next(
                        "div", class_="spec-value"
                    )
                    if type(property_value_block) is Tag:
                        property_value = property_value_block.text
                        properties[property_name.text] = property_value
                try:
                    elements_count = int(
                        re.findall(r"\d+", properties["Количество деталей:"])[0]
                    )
                except KeyError:
                    elements_count = 0
                if elements_count < 15:
                    try:
                        elements_count = re.findall(r"(\d+)-detalej", url)[0]
                    except IndexError:
                        elements_count = 0
                try:
                    sizes = [
                        float(size)
                        for size in re.findall(
                            r"\d+\.\d+|\d+",
                            properties["Размер пазла:"].replace(",", "."),
                        )
                    ]
                except KeyError:
                    sizes = [0, 0]
                if len(sizes) > 2:
                    return
                width, height = sizes
                try:
                    manufacturer = properties["Производитель:"].split()[0]
                    try:
                        country = properties["Производитель:"].split("(")[1][:-1]
                    except IndexError:
                        country = ""
                except KeyError:
                    try:
                        manufacturer = re.findall(r"азл (\w+) ", title_block.text)[0]
                    except IndexError:
                        manufacturer = ""
                    country = ""
                image_url = self.__HOST_URL + str(image_block.get("href"))
                image_path = (
                    self.__SAVE_IMAGES_DIRECTORY_PATH + image_url.split("/")[-1]
                )
                await self.__download_image(session, image_url)
                self.__puzzles.append(
                    Puzzle(
                        article_number,
                        title,
                        elements_count,
                        width,
                        height,
                        manufacturer,
                        country,
                        image_path,
                        url,
                    )
                )
                self.__completed_puzzles_count += 1
                print(
                    f"ПАЗЛЫ: Обработано {self.__completed_puzzles_count}, осталось {self.__puzzles_count-self.__completed_puzzles_count}"
                )

    async def __download_image(self, session: ClientSession, url: str):
        image_simple_file_name = url.split("/")[-1]
        image_file_path = Path.joinpath(
            Path(self.__SAVE_IMAGES_DIRECTORY_PATH), image_simple_file_name
        )
        if not Path.is_file(image_file_path):
            while True:
                try:
                    async with session.get(url) as response:
                        image_bytes = await response.read()
                    break
                except ClientConnectionError:
                    await asyncio.sleep(1)
            while True:
                try:
                    async with aiofiles.open(image_file_path, "wb") as image_file:
                        await image_file.write(image_bytes)
                    break
                except OSError:
                    await asyncio.sleep(1)
            print(f"ИЗОБРАЖЕНИЯ: Загружена {image_simple_file_name}")

    async def __save(self):
        file_name = datetime.now().strftime(r"%d_%m_%Y_%H_%M_%S")
        while True:
            try:
                async with aiofiles.open(
                    Path.joinpath(Path(self.__SAVE_DIRECTORY_PATH), f"{file_name}.csv"),
                    "w",
                ) as csv_file, aiofiles.open(
                    Path.joinpath(Path(self.__SAVE_DIRECTORY_PATH), f"{file_name}.sql"),
                    "w",
                ) as sql_file:
                    write_csv_task = csv_file.write(self.__get_csv(";"))
                    write_sql_task = sql_file.write(self.__get_sql("table_name"))
                    await write_csv_task
                    await write_sql_task
                break
            except OSError:
                await asyncio.sleep(1)
        print(
            f"ПРОЦЕСС ЗАВЕРШЁН\nОбработано:\n\tСтраниц:{self.__pages_count}\n\tПазлов:{self.__puzzles_count}\nДанные сохранены по пути: {self.__SAVE_DIRECTORY_PATH}"
        )

    def __get_csv(self, separator: str) -> str:
        csv = "article_number;title;elements_count;width;height;manufacturer;country;image_path;url\n"
        for puzzle in self.__puzzles:
            csv += puzzle.get_csv(separator) + "\n"
        return csv

    def __get_sql(self, table_name: str) -> str:
        sql = ""
        for puzzle in self.__puzzles:
            sql += puzzle.get_sql(table_name) + "\n"
        return sql

    async def parse(self):
        print("Запуск парсера")
        self.__completed_pages_count = 0
        self.__completed_puzzles_count = 0
        await self.__create_open_pages_tasks()
        await self.__save()
