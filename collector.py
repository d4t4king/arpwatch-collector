#!/usr/bin/python

import argparse
import pprint

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-t', '--target-file', dest='target_file', help="The list of end points to colect from")
args = args_parse.parse_args()


