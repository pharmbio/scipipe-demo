#!/bin/bash -l
#SBATCH -A snic2017-7-89 -p core -n 8 -t 8:00:00 -J UnTgzApps
./cawpre -maxtasks 1 -procs untgz_apps
