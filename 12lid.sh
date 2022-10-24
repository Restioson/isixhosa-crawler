#!/bin/bash

cd NCHLT\ South\ African\ Language\ Identifier/linux/

for PORT in {7770..7781}
do
  screen -d -m ./salid server -p $PORT
done

read -r -d '' _ </dev/tty
pgrep -P $$ --signal SIGTERM
