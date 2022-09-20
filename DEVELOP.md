# Publish

```bash
rm -rf build dist && python setup.py sdist bdist_wheel

# pypi
twine upload dist/*
# local repo
twine upload -r local dist/*

```

or

```bash
chmod +x ./publish.sh
./publish.sh
```

this will only publish to pypi repo
