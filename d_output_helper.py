import os
import shutil
import sys
from distutils.dir_util import copy_tree


def main(argv):
    args_formatted = ' '.join(argv)
    args_formatted = args_formatted.replace(" =", "=")
    args_formatted = args_formatted.replace("= ", "=")

    dict_options = {}
    for option in args_formatted.split()[1:]:
        key = option
        value = None
        if "=" in option:
            split_by_equal = option.split("=")
            key = split_by_equal[0]
            value = split_by_equal[1]
        dict_options[key] = value
    output_dir = get_output_directory(dict_options)
    rm_all_option = get_rm_all_option(dict_options)
    use_cache_option = get_use_cache_option(dict_options)
    cache_dir = get_cache_dir_option(dict_options)
    entities_delete = get_entities_delete_option(dict_options)
    dirs_delete = get_dirs_delete_option(dict_options)
    options = {
        "output_dir": output_dir,
        "rm_all_option": rm_all_option,
        "use_cache_option": use_cache_option,
        "cache_dir": cache_dir,
        "entities_delete": entities_delete,
        "dirs_delete": dirs_delete
    }
    apply_options(options)


def apply_options(options):
    output_dir = options.get("output_dir")
    rm_all_option = options.get("rm_all_option")
    use_cache_option = options.get("use_cache_option")
    cache_dir = options.get("cache_dir")
    entities_delete = options.get("entities_delete")
    dirs_delete = options.get("dirs_delete")

    print("Getting ready to remove files in:", output_dir)

    if rm_all_option:
        delete_all_files_in_directory(output_dir)
    elif entities_delete:
        delete_files_for_entities(entities_delete, dirs_delete, output_dir)

    if use_cache_option:
        copy_tree(cache_dir, output_dir)


def delete_all_files_in_directory(directory):
    if os.path.exists(directory):
        print("delete dir", directory)
        shutil.rmtree(directory)


def delete_file(file):
    if os.path.exists(file):
        print("delete file", file)
        os.remove(file)


def delete_files_for_entities(entities_delete, dirs_delete, output_dir):
    output_dir_path = output_dir
    if not output_dir_path.endswith("/"):
        output_dir_path = output_dir_path + "/"
    for entity in entities_delete:
        for directory in dirs_delete:
            if directory in ["database"]:
                path = output_dir_path + directory + "/copied/" + entity
                delete_file(path)
                delete_file(output_dir_path + directory + "/fks_indexes_created")
                delete_file(output_dir_path + directory + "/fks_indexes_deleted")
                delete_all_files_in_directory(output_dir_path + directory + "/reports/")
            else:
                path = output_dir_path + directory + "/" + entity
                delete_all_files_in_directory(path)


def get_output_directory(dict_options):
    output_dir = "output"
    if "--output-dir" in dict_options:
        value = dict_options["--output-dir"]
        if value:
            output_dir = value
    return output_dir


def get_rm_all_option(dict_options):
    return "--rm_all" in dict_options


def get_use_cache_option(dict_options):
    return "--use-cache" in dict_options


def get_cache_dir_option(dict_options):
    cache_dir = "cache"
    if "--cache-dir" in dict_options:
        value = dict_options["--cache-dir"]
        if value:
            cache_dir = value
    if not os.path.exists(cache_dir):
        raise Exception("Directory for cache [{0}] does not exist".format(cache_dir))
    return cache_dir


def get_entities_delete_option(dict_options):
    entities_delete = None
    if "--entities_delete" in dict_options:
        value = dict_options["--entities_delete"]
        value = value.lower()
        if value:
            entities = value.split(",")
            entities_delete = entities
    return entities_delete


def get_dirs_delete_option(dict_options):
    dirs_delete = []
    if "--dirs_delete" in dict_options:
        value = dict_options["--dirs_delete"]
        value = value.lower()
        if value:
            dirs = value.split(",")
            dirs_delete = dirs
    return dirs_delete


if __name__ == "__main__":
    sys.exit(main(sys.argv))
