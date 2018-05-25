#!/bin/bash -l
#SBATCH -A snic2017-7-89 
#SBATCH -p node
#SBATCH -t 1-00:00:00
#SBATCH -J cawpre_scipipe_test
binname=cawpre_$(date +%Y%m%d_%H%M%S)
go build -o $binname
./$binname -maxtasks 11 -procs print_reads_.*
