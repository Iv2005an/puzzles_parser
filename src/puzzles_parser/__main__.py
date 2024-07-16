import asyncio

from puzzles_parser.parser import PuzzlesParser


def main():
    asyncio.run(PuzzlesParser().parse())


if __name__ == "__main__":
    main()
