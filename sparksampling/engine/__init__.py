# import all .py from in this dir
from os.path import dirname, basename, isfile, join
import glob
import importlib

modules = glob.glob(join(dirname(__file__), "*.py"))
sub_packages = (basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py'))
packages = (str(__package__) + '.' + i for i in sub_packages)
[importlib.import_module(p) for p in packages]
