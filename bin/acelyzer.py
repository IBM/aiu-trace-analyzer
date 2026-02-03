# Copyright 2024-2025 IBM Corporation


import sys

from aiu_trace_analyzer.core.acelyzer import Acelyzer


_min_version = (3, 9)
if sys.version_info.major < _min_version[0] or sys.version_info.minor < _min_version[1]:
    print(f'ERROR: Python version {_min_version[0]}.{_min_version[1]}'
          f' or newer is required. Detected: {sys.version_info.major}.{sys.version_info.minor}')
    sys.exit(1)


# set to true to profile event processing
ENABLE_TOOL_PROFILER = False


def main(input_args=None) -> None:
    '''
    Entry point for the acelyzer command-line tool.

    Initializes and runs the Acelyzer trace analyzer to process and analyze
    AIU trace files. This function serves as a wrapper that creates an Acelyzer
    instance and executes the trace processing pipeline.

    Args:
        input_args: Optional list of command-line arguments to parse.
                   If None, arguments are read from sys.argv.
                   Example: ['--input', 'trace.json', '--output', 'result.json']

    Returns:
        None
    '''
    acelyzer = Acelyzer(input_args)
    acelyzer.run()


if __name__ == "__main__":
    if ENABLE_TOOL_PROFILER:
        import cProfile
        cProfile.run("main()")
    else:
        main()
