package main

import (
	sp "github.com/scipipe/scipipe"
)

// ShuffleLines shuffles the lines in the file on the InData in-port, based on
// random bytes in a file on the InRandBytes in-port
type ShuffleLines struct {
	*sp.Process
}

// ShuffleLinesConf contains parameters for initializing a
// ShuffleLines process
type ShuffleLinesConf struct {
}

// NewShuffleLines returns a new ShuffleLines process
func NewShuffleLines(wf *sp.Workflow, name string, params ShuffleLinesConf) *ShuffleLines {
	cmd := `shuf ` +
		`--random-source={i:randbytes} ` +
		`{i:in} ` +
		`> {o:shuffled}`
	p := wf.NewProc(name, cmd)
	p.SetOut("shuffled", "{i:in}.shuf")
	return &ShuffleLines{p}
}

// InData returns the in-port
func (p *ShuffleLines) InData() *sp.InPort {
	return p.In("in")
}

// InRandBytes returns the in-port
func (p *ShuffleLines) InRandBytes() *sp.InPort {
	return p.In("randbytes")
}

// OutShuffled returns the Shuffled out-port
func (p *ShuffleLines) OutShuffled() *sp.OutPort {
	return p.Out("shuffled")
}
