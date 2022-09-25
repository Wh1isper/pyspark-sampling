# Publish

```bash
rm -rf build dist && python setup.py sdist bdist_wheel

# pypi
twine upload dist/*
# local repo
twine upload -r local dist/*

```

# Local Development

```bash
# get main package location
python -c "import sparksampling;print(sparksampling.__path__[0])"

# link it to main package
ln -s ${PWD}/sparksampling/evaluation_extension/{EXTENSION_NAME} ${SPARK_SAMPLING_PATH}/evaluation_extension/

```

# How to make an extension package

``` bash
├── setup.py
├── sparksampling                                       # do not change it
│   └── __init__.py
│   ├── evaluation_extension                      # do not change it
│   │   └── __init__.py
│   │   ├── example_extension               # your extension package start from here
│   │   │   ├── example_extension.py
│   │   │   ├── __init__.py

```
