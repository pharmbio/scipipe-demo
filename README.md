# Case study workflows for SciPipe

A few case study workflows for the [SciPipe](http://scipipe.org) publication.

## Usage

1. Navigate into each of the case study workflow folders.

2. Execute the `run.sh` script:

```bash
./run.sh
```
## Prerequisites

All workflows require:

- A unix like operating system (Linux or Mac)
- The Go tool chain.
  - See [this link](https://golang.org/dl/) for instructions on downloading and
    installing Go.

The cancer analysis, and RNA-seq workflows require a few bioinformatics tools installed on the system:

- bwa
- samtools

On Ubuntu, these can be installed with this command:

```bash
sudo apt-get install -y samtools bwa
```

The RNA-Seq workflow requires Python 2.7.x.

## Resource requirements

The RNA-seq and Drug Discovery workflows should be runnable on a reasonably
modern laptop with at least a few GB of storage available.

The Cancer analysis workflow on the other hand, requires a large amount of
RAM memory, at least 16GB.

The number of cores used, can be modified on each of the workflows, by
editing the `run.sh` script and adding or modifying the `-taxtasks [n]` flag.
E.g:

```bash
go run workflow.go -maxtasks 4
```

or

```bash
go build -o workflow
./workflow -maxtasks 4
```
