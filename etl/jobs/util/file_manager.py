import os
from distutils.dir_util import copy_tree


def copy_directory(deploy_mode, source, destination):
    if not os.path.exists(source):
        raise Exception("Source directory for cache [{0}] does not exist".format(source))
    print("deploy_mode::", deploy_mode)
    if "local" == deploy_mode:
        print("Local copy")
        copy_tree(source, destination)
    else:
        raise Exception("Deploy mode [{0}] not supported yet for copying directories".format(deploy_mode))

    print("Copied cache from {0} to {1}".format(source, destination))

