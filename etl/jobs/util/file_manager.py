import os
from distutils.dir_util import copy_tree


def copy_directory(source, destination):
    if not os.path.exists(source):
        raise Exception("Source directory for cache [{0}] does not exist".format(source))
    copy_tree(source, destination)

    print("Copied cache from {0} to {1}".format(source, destination))

