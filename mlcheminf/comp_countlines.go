package main

import (
	sp "github.com/scipipe/scipipe"
)

// CountLines does blabla ...
type CountLines struct {
	*sp.Process
}

// CountLinesConf contains parameters for initializing a
// CountLines process
type CountLinesConf struct {
	UnGzip bool
}

// NewCountLines returns a new CountLines process
func NewCountLines(wf *sp.Workflow, name string, params CountLinesConf) *CountLines {
	cmd := `cat {i:in} | wc -l > {o:linecnt}`
	if params.UnGzip {
		cmd = "z" + cmd
	}
	p := wf.NewProc(name, cmd)
	p.SetOut("linecnt", "{i:in}.linecnt")
	return &CountLines{p}
}

// InFile returns the Infile in-port
func (p *CountLines) InFile() *sp.InPort {
	return p.In("in")
}

// OutLineCount returns the LineCount out-port
func (p *CountLines) OutLineCount() *sp.OutPort {
	return p.Out("linecnt")
}
