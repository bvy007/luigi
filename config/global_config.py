import os, sys

"""  Code to set up the programming environment  """

defaults = {
    'package_dir' : '/home/bvy007/trusted_shops_assignment',
    'config_dir' : 'config/'
}

envlist = ['package_dir', 'config_dir']

for env in envlist:
    if env not in os.environ:
        os.environ[env] = defaults[env] 
        print("Environment Variable: " + str(env) + " has not been set to default: " + str(os.environ[env]))

package_dir = os.environ['package_dir']
config_dir = os.environ['config_dir']