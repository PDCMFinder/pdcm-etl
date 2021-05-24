import os


def get_not_empty_files(file_paths):
    paths_with_non_empty_files = []
    for path in file_paths:
        if not is_file_emtpy(path):
            paths_with_non_empty_files.append(path)
    return paths_with_non_empty_files


def is_file_emtpy(file):
    return os.stat(file).st_size == 0
