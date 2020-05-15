#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 12:01:06 2020

@author: ageiges
"""
import os
import platform
OS = platform.system()


def change_personal_config():
    from .tools.install_support import create_personal_setting
    modulePath =  os.path.dirname(__file__) + '/'
    create_personal_setting(modulePath, OS)