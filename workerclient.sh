#!/bin/bash

set -e

cmds=(
      "./worker --listen 127.0.0.1:20003 --id worker0"
      "./worker --listen 127.0.0.1:20000 --id worker1"
      "./worker --listen 127.0.0.1:20001 --id worker2"
      "./worker --listen 127.0.0.1:20002 --id worker3"
      "./worker --listen 127.0.0.1:20004 --id worker4"
      "./worker --listen 127.0.0.1:20005 --id worker5"
      "./worker --listen 127.0.0.1:20006 --id worker6"
      "./worker --listen 127.0.0.1:20007 --id worker7"
      "./worker --listen 127.0.0.1:20008 --id worker8"
      "./worker --listen 127.0.0.1:20009 --id worker9"
      "./worker --listen 127.0.0.1:20010 --id worker10"
      "./worker --listen 127.0.0.1:20011 --id worker11"
      "./worker --listen 127.0.0.1:20012 --id worker12"
      "./worker --listen 127.0.0.1:20013 --id worker13"
      "./worker --listen 127.0.0.1:20014 --id worker14"
      "./worker --listen 127.0.0.1:20015 --id worker15"
#      "./worker --listen 127.0.0.1:20016 --id worker16"
#      "./worker --listen 127.0.0.1:20017 --id worker17"
#      "./worker --listen 127.0.0.1:20018 --id worker18"
#      "./worker --listen 127.0.0.1:20019 --id worker19"
#      "./worker --listen 127.0.0.1:20020 --id worker20"
#      "./worker --listen 127.0.0.1:20021 --id worker21"
#      "./worker --listen 127.0.0.1:20022 --id worker22"
#      "./worker --listen 127.0.0.1:20023 --id worker23"
      "./client"
      )

for cmd in "${cmds[@]}"; do {
  $cmd & pid=$!
  PID_LIST+=" $pid";
  sleep .1
} done

trap "kill $PID_LIST" SIGINT

wait $PID_LIST
