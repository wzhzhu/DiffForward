#!/usr/bin/python2
#coding:utf-8
import configparser
import os

class ConfigReader:
    def __init__(self, filepath='config.ini'):
        configpath = filepath
        self.cf = configparser.ConfigParser()
        self.cf.read(configpath)

    def get(self, section, option):
        value = self.cf.get(section, option)
        return value