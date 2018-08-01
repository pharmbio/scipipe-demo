#!/bin/bash -l
#SBATCH -A snic2017-7-89 
#SBATCH -p node
#SBATCH -t 1-00:00:00
#SBATCH -J cawpre_scipipe_test
module load bioinfo-tools
module load samtools/1.8
module load bwa/0.7.17
binname=cawpre_$(date +%Y%m%d_%H%M%S)
go build -o $binname
./$binname -maxtasks 11 -procs "print_reads_.*"
