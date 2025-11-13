#!/bin/bash
DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
cd $DIR
source mcp_env/bin/activate
#python3 http_server.py
daemonize \
	-a \
	-e $DIR/server.stderr \
	-o $DIR/server.stdout \
	-c $DIR \
	-v \
	-p $DIR/server.pid \
	$DIR/mcp_env/bin/python3 $DIR/http_server.py
