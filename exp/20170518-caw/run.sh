#!/bin/bash -l
module load bioinfo-tools; module load bwa/0.7.15 samtools/1.4 GATK/3.6
go run caw-workflow.go
