# Copyright 2024-2025 IBM Corporation

import datetime


class _LogLevel(object):
    def __repr__(self):
        raise NotImplementedError


class _ERROR(_LogLevel):
    def __repr__(self):
        return "ERROR"


class _WARNING(_LogLevel):
    def __repr__(self):
        return "WARNING"


class _INFO(_LogLevel):
    def __repr__(self):
        return "INFO"


class _DEBUG(_LogLevel):
    def __repr__(self):
        return "DEBUG"


class _TRACE(_LogLevel):
    def __repr__(self):
        return "TRACE"


def string_to_loglevel(loglevel_str):
    loglevel_str = loglevel_str.strip()
    if loglevel_str == "ERROR":
        return _ERROR()
    elif loglevel_str == "WARNING":
        return _WARNING()
    elif loglevel_str == "INFO":
        return _INFO()
    elif loglevel_str == "DEBUG":
        return _DEBUG()
    elif loglevel_str == "TRACE":
        return _TRACE()
    else:
        raise ValueError("Un expected log level: %s" % (loglevel_str))


def int_to_loglevel(loglevel_int):
    if loglevel_int == 0:
        return _ERROR()
    elif loglevel_int == 1:
        return _WARNING()
    elif loglevel_int == 2:
        return _INFO()
    elif loglevel_int == 3:
        return _DEBUG()
    elif loglevel_int == 4:
        return _TRACE()
    else:
        raise ValueError("Un expected log level: %s" % (loglevel_int))


# log-level definitions as ints
ERROR = 0
WARN = 1
INFO = 2
DEBUG = 3
TRACE = 4


# default global log warning
loglevel = WARN


# update the loglevel to 'll'
def setloglevel(ll: int):
    global loglevel
    assert (ll >= ERROR and ll <= TRACE)
    loglevel = ll


logcolor_codes = [
    ("\033[91m", "\033[0m"),   # red error
    ("\033[93m", "\033[0m"),   # yellow warning
    ("", ""),           # everything else: nothing
    ("", ""),
    ("", "")
]


# logging function
def log(level: int, *args):
    if level <= loglevel:
        ts = datetime.datetime.now().isoformat()
        logcolor, reset_color = logcolor_codes[level]
        print(f"{ts} {logcolor}{'{0: >8}'.format(int_to_loglevel(level).__repr__())}{reset_color}", end=" ")
        for arg in args:
            print(arg, end=" ")
        print()
