# Copyright 2024-2025 IBM Corporation


import sys

from aiu_trace_analyzer.core.acelyzer import Acelyzer


_min_version = (3, 9)
if sys.version_info.major < _min_version[0] or sys.version_info.minor < _min_version[1]:
    print(f'ERROR: Python version {_min_version[0]}.{_min_version[1]}'
          f' or newer is required. Detected: {sys.version_info.major}.{sys.version_info.minor}')
    sys.exit(1)


# set to true to profile event processing
enable_tool_profiler = False


def main(input_args=None) -> None:
    # this is just a wrapper, processing runs as part of instance creation right now
    acelyzer = Acelyzer(input_args)
    acelyzer.run()


if __name__ == "__main__":
    if enable_tool_profiler:
        import cProfile
        cProfile.run("main()")
    else:
        main()
