#!/bin/bash

#use this script to stop all jsvc daemons 
#if you are karnak account, just firing 'kstop' will do the same thing as this script: killing all jsvc processes
/usr/bin/jsvc -stop -pidfile /home/karnak/karnak/var/webservice.pid  karnak.service.WebService
/usr/bin/jsvc -stop -pidfile /home/karnak/karnak/var/trainer.pid karnak.service.train.Trainer
