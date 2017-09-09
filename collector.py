#!/usr/bin/python

import argparse
import pprint
import os.path
import paramiko

pp = pprint.PrettyPrinter(indent=4)

args_parse = argparse.ArgumentParser(description="Get those ARPs!")
args_parse.add_argument('-t', '--target-file', dest='target_file', help="The list of end points to colect from")
args = args_parse.parse_args()

def process_file(data_blob):
	for l in data_blob.splitlines():
		

if os.path.isfile(args.target_file):
	with open(args.target_file, 'r') as f:
		lines = f.read().splitlines()

# prepare the SSH client
#client = paramiko.SSHClient()
#client.load_system_host_keys()
#client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#for h in lines:
#	h.lstrip()
#	h.rstrip()
#	client.connect(h, username='root', key_filename='/root/.ssh/id_rsa')
#	pp.pprint(client)
#	break


