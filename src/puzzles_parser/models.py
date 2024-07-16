from dataclasses import dataclass


@dataclass(frozen=True, unsafe_hash=True)
class Category:
    name: str
    url: str

    def __str__(self) -> str:
        return f"{self.name}: {self.url}"


@dataclass(frozen=True)
class Puzzle:
    article_number: int
    title: str
    elements_count: int
    width: float
    height: float
    manufacturer: str
    country: str
    image_path: str
    url: str

    def get_csv(self, separator: str) -> str:
        return (
            f'"{self.article_number}"{separator}'
            + f'"{self.title}"{separator}'
            + f'"{self.elements_count}"{separator}'
            + f'"{self.width}"{separator}'
            + f'"{self.height}"{separator}'
            + f'"{self.manufacturer}"{separator}'
            + f'"{self.country}"{separator}'
            + f'"{self.image_path}"{separator}'
            + f'"{self.url}"'
        )

    def get_sql(self, table_name: str):
        return (
            f"INSERT INTO {table_name} VALUES("
            + f"{self.article_number},"
            + f"'{self.title}',"
            + f"{self.elements_count},"
            + f"{self.width},"
            + f"{self.height},"
            + f"'{self.manufacturer}',"
            + f"'{self.country}',"
            + f"'{self.image_path}',"
            + f"'{self.url}');"
        )
