#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Checks to see if necessary modules exist
"""
import os

MODULE_LIST = [
    'argparse', 'configparser', 'datetime', 'os',
    'sys', 'json', 'pandas', 'requests'
]

ISSUES = 0

currentdir = os.path.abspath(os.path.dirname(__file__))
modulecfg = f'{currentdir}/requirements.txt'

if os.path.exists(modulecfg):
    with open ( modulecfg, "r", encoding='utf8') as cfgobject:
        MODULE_LIST = cfgobject.readlines()

for MY_MODULE in MODULE_LIST:
    MY_MODULE = MY_MODULE.rstrip()
    print(f'# Module # {MY_MODULE}')
    try:
        __import__(MY_MODULE)
    except ImportError:
        print(f'### Issue ### ToFix ### pip3 install {MY_MODULE}')
        ISSUES = ISSUES + 1
print(f'# Report # {ISSUES} Issues')
