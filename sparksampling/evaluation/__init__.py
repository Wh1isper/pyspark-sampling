# import all .py from in this dir
import glob
import importlib
import os
from os.path import basename, dirname, isfile, join

modules = glob.glob(join(dirname(__file__), "*.py"))
sub_packages = (basename(f)[:-3] for f in modules if isfile(f) and not f.endswith("__init__.py"))
packages = (str(__package__) + "." + i for i in sub_packages)
[importlib.import_module(p) for p in packages]

evaluation_module_name = "evaluation_extension"
evaluation_extension_path = os.path.join(os.path.dirname(__file__), f"../{evaluation_module_name}")
evaluation_sub_folders = (f.name for f in os.scandir(evaluation_extension_path) if f.is_dir())
sub_packages = (f for f in evaluation_sub_folders if not (f.endswith("__") or f.startswith("__")))
evaluation_package = ".".join((str(__package__).split("."))[:-1])
packages = [evaluation_package + "." + evaluation_module_name + "." + i for i in sub_packages]
[importlib.import_module(p) for p in packages]
