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


