#!/bin/bash
DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
cd $DIR
pid=$(cat $DIR/server.pid)
kill $pid
