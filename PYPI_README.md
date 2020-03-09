# Releasing on PyPI

We follow something along the lines of the 2017 version of
https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty/.


## One-time setup

### .pypirc
Make sure your `~/.pypirc` is set up like this

```bash
[distutils]
index-servers=
    pypi
    test

[test]
repository = https://test.pypi.org/legacy/
username = <your test user name goes here>

[pypi]
username = __token__
```

### pip installs

```bash
pip install twine -U
```

## Build
```bash
rm -rf build dist
python setup.py sdist bdist_wheel
```

## Push to PyPI staging server

```bash
twine upload -r test --sign dist/*
```

In a different virtualenv, test that you can install it:

```bash
pip install -i https://testpypi.python.org/pypi jsonobject-couchdbkit --upgrade
```


## Push to PyPI

```bash
twine upload -r pypi --sign dist/*
```
