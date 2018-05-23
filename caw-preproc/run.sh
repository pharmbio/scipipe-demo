#!/bin/bash -l
module load bioinfo-tools; module load bwa/0.7.15 samtools/1.4
time go run caw.go 2>&1 | tee log/run-$(date +%Y%m%d-%H%M%S).log
