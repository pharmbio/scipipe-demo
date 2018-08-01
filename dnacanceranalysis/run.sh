#!/bin/bash -l
go run dnacanceranalysiswf.go -maxtasks 11 -procs "print_reads_.*"
