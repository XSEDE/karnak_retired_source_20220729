#!/bin/bash

#  -nodetach \
/usr/bin/jsvc \
  -cp $HOME/karnak/jvm/target/karnak-assembly-0.1.jar \
  -pidfile $HOME/karnak/var/webservice.pid \
  -outfile $HOME/karnak/var/webservice.log \
  -errfile '&1' \
  -Dlog4j.configuration=file://$HOME/karnak/etc/log4j.properties \
  karnak.service.WebService
