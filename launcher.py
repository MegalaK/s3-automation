import pytest
import readProperties
import test_Main_code
import sys
import os

Config=readProperties.ReadConfig

def test_valueConsolidation():
    test_Main_code.valueconsolidation()

def test_move_file():
    test_Main_code.move_file(Config,getData("Queries","Archive_path"), Config.getData("Queries","html_path"))

def test_Athena_compare():
    test_Main_code.Athean_view_comparison()
  
