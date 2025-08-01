# Copyright 2024-2025 IBM Corporation

from pathlib import Path


# define TraceEvent to be a dictionary for consistency
class TraceEvent(dict):
    pass


class InputDialect:
    dialect_map = {}
    categories = set()

    @classmethod
    def register(cls, category: str, entry: str) -> bool:
        if category not in cls.categories:
            raise KeyError(f"ERROR: Category {category} is not part of this dialect.")

        cls.dialect_map[category] = entry
        return True

    @classmethod
    def add_category(cls, category: str) -> bool:
        if category in cls.categories:
            return False
        cls.categories.add(category)
        return True


class InputDialectFLEX(InputDialect):
    _FLEX_DIALECT = {
        "NAME": "FLEX",
        "AIU_Runtime": "AIU_Roundtrip",
    }

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
            for c, e in cls._FLEX_DIALECT.items():
                cls.register(c, e)
        return super(InputDialectFLEX, cls).__new__(cls)


class InputDialectTORCH(InputDialect):
    _TORCH_DIALECT = {
        "NAME": "TORCH",
        "AIU_Runtime": "AIU_Runtime",
    }

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
            for c, e in cls._TORCH_DIALECT.items():
                cls.add_category(c)
                cls.register(c, e)
        return super(InputDialectTORCH, cls).__new__(cls)


class GlobalIngestData(object):
    _jobmap = None

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
        return super(GlobalIngestData, cls).__new__(cls)

    @classmethod
    def add_job_info(cls, source_uri: str, data_dialect: InputDialect = None) -> int:
        jobhash = hash(source_uri) % 10000
        if jobhash not in cls._jobmap:
            cls._jobmap[jobhash] = (Path(source_uri).name, data_dialect)
        return jobhash

    @classmethod
    def get_job(cls, jobhash: int) -> str:
        try:
            return cls._jobmap[jobhash][0]
        except KeyError:
            print("jobmap empty")
            return "Not Available"
