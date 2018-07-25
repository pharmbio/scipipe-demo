package main

import (
	"fmt"
	"log"
	"time"
)

type PartitionType string

const (
	PartitionCore PartitionType = "core"
	PartitionNode PartitionType = "node"
)

type RunMode int

const (
	RunModeLocal RunMode = iota
	RunModeHPC   RunMode = iota
	RunModeMPI   RunMode = iota
)

// SlurmInfo contains info needed to launch a job on a SLURM cluster
type SlurmInfo struct {
	Project   string
	Partition PartitionType
	Cores     int
	Time      time.Duration
	JobName   string
	Threads   int
}

func (si SlurmInfo) AsSallocString() string {
	return fmt.Sprintf("salloc -A %s -p %s -n %d -t %s -J %s srun -n 1 -c %d ",
		si.Project,
		si.Partition,
		si.Cores,
		fmtDuration(si.Time),
		si.JobName,
		si.Threads)
}

func fmtDuration(t time.Duration) string {
	t = t.Round(time.Second)
	d := t / (24 * time.Hour)
	t -= d * (24 * time.Hour)
	h := t / time.Hour
	t -= h * time.Hour
	m := t / time.Minute
	t -= m * time.Minute
	s := t / time.Second
	return fmt.Sprintf("%d-%02d:%02d:%02d", d, h, m, s)
}

func parseDuration(durStr string) time.Duration {
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		log.Fatal(err)
	}
	return dur
}

// fs is a short for fmt.Sprintf
func fs(pat string, v ...interface{}) string {
	return fmt.Sprintf(pat, v...)
}
