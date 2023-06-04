
"""  Code to load the yaml files  """

import yaml
import os

from config.global_config import config_dir

verbose = False

with open(os.path.join(config_dir, 'arguments.yaml'), 'r') as stream:
    try:
        project_arguments = yaml.load(stream, Loader=yaml.FullLoader)
        if verbose:
            print("Read arguments.yaml:" + str(project_arguments))
    except yaml.YAMMLError as exc:
        print(exc)