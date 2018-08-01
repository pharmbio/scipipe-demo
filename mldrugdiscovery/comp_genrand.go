package main

import (
	sp "github.com/scipipe/scipipe"
)

// GenRandBytes does blabla ...
type GenRandBytes struct {
	*sp.Process
}

// GenRandBytesConf contains parameters for initializing a
// GenRandBytes process
type GenRandBytesConf struct {
	SizeMB      int
	ReplicateID string
}

// NewGenRandBytes returns a new GenRandBytes process
func NewGenRandBytes(wf *sp.Workflow, name string, params GenRandBytesConf) *GenRandBytes {
	cmd := `dd ` +
		`if=/dev/urandom ` +
		`of={o:randbytes} ` +
		`bs=1048576 ` +
		`count={p:sizemb} # {i:basepath}`
	p := wf.NewProc(name, cmd)
	p.InParam("sizemb").FromInt(params.SizeMB)
	p.InParam("replid").FromStr(params.ReplicateID)
	p.SetOut("randbytes", "{i:basepath}.{p:replid}.rand")
	return &GenRandBytes{p}
}

// InBasePath returns the BasePath in-port
func (p *GenRandBytes) InBasePath() *sp.InPort {
	return p.In("basepath")
}

// OutRandBytes returns the RandBytes out-port
func (p *GenRandBytes) OutRandBytes() *sp.OutPort {
	return p.Out("randbytes")
}
