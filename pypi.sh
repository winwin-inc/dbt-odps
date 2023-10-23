#!/bin/bash

# build
rm -rf build dist winwin.egg-info
python setup.py sdist bdist_wheel

# upload pypi.org [--verbose]
# 配置Token 或者 用户名/密码
python -m twine upload dist/* &&
  echo "INFO: upload [pypi.org] success!!!"