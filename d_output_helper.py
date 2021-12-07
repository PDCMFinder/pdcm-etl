import argparse
import os
import shutil
import sys


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--output-dir', help='Directory where the output for the ETL is located.')
    parser.add_argument('--rm_all', help='If set, deletes all the data in the output-dir.', action="store_true")
    parser.add_argument('--entities', help='Entities for which the data will be deleted.')
    parser.add_argument('--dirs', help='Directories to be deleted in the entities list.'
                                       'Allowed values: [raw, transformed, database_formatted, database]')
    args = parser.parse_args()
    output_dir = args.output_dir
    rm_all_option = args.rm_all
    entities_delete = get_entities_delete_option(args.entities)
    dirs_delete = get_dirs_delete_option(args.dirs)
    print("dirs_delete", dirs_delete)
    process(output_dir, rm_all_option, entities_delete, dirs_delete)


def process(output_dir, rm_all_option, entities_delete, dirs_delete):
    if not output_dir:
        raise Exception("Output dir not provided")
    print("Getting ready to remove files in:", output_dir)

    if rm_all_option:
        delete_all_files_in_directory(output_dir)
    elif entities_delete:
        delete_files_for_entities(entities_delete, dirs_delete, output_dir)


def delete_all_files_in_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        print("Deleted dir", directory)


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)
        print("Deleted file", file)


def delete_files_for_entities(entities_delete, dirs_delete, output_dir):
    output_dir_path = output_dir
    if not output_dir_path.endswith("/"):
        output_dir_path = output_dir_path + "/"
    for entity in entities_delete:
        for directory in dirs_delete:
            print("processing", directory)
            if directory in ["database"]:
                path = output_dir_path + directory + "/copied/" + entity
                delete_file(path)
                delete_file(output_dir_path + directory + "/fks_indexes_created")
                delete_file(output_dir_path + directory + "/fks_indexes_deleted")
                delete_all_files_in_directory(output_dir_path + directory + "/reports/")
            else:
                path = output_dir_path + directory + "/" + entity
                delete_all_files_in_directory(path)


def get_entities_delete_option(entities):
    print("entities", entities)
    entities_delete = []
    if entities:
        entities_delete = entities.lower().split(",")
    print("entities_delete", entities_delete)
    return entities_delete


def get_dirs_delete_option(dirs):
    dirs_delete = []
    if dirs:
        dirs_delete = dirs.lower().split(",")
    return dirs_delete


if __name__ == "__main__":
    sys.exit(main())
