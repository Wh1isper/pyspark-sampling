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

In setup.py, make sure extension package is **subpackage** of `sparksampling.evaluation_extension`

```python
setup(
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="sparksampling-evaluation-extension-example",
    install_requires=requirements,
    include_package_data=True,
    keywords='sparksampling-evaluation-extension-example',
    name='sparksampling-evaluation-extension-example',
    packages=find_packages(include=['sparksampling.evaluation_extension.example_extension']), # using subpackage here
    version='0.1.0',
    zip_safe=False,
)
```

# TODO

- cookiecutter for extension
