package main

import (
	sp "github.com/scipipe/scipipe"
)

// CreateSparseTrain does blabla ...
type CreateSparseTrain struct {
	*sp.Process
}

// CreateSparseTrainConf contains parameters for initializing a
// CreateSparseTrain process
type CreateSparseTrainConf struct {
	ReplicateID string
}

// NewCreateSparseTrain returns a new CreateSparseTrain process
func NewCreateSparseTrain(wf *sp.Workflow, name string, params CreateSparseTrainConf) *CreateSparseTrain {
	cmd := `java -jar ../bin/CreateSparseDataset.jar \
	-inputfile {i:traindata} \
	-datasetfile {o:sparsetrain} \
	-signaturesoutfile {o:signatures} \
	-silent`
	p := wf.NewProc(name, cmd)
	p.SetOut("sparsetrain", "{i:traindata}.csr")
	p.SetOut("signatures", "{i:traindata}.sign")
	p.SetOut("log", "{i:traindata}.csr.log")
	return &CreateSparseTrain{p}
}

// InTraindata returns the Traindata in-port
func (p *CreateSparseTrain) InTraindata() *sp.InPort {
	return p.In("traindata")
}

// OutSparseTraindata returns the SparseTraindata out-port
func (p *CreateSparseTrain) OutSparseTraindata() *sp.OutPort {
	return p.Out("sparsetrain")
}

// OutSignatures returns the Signatures out-port
func (p *CreateSparseTrain) OutSignatures() *sp.OutPort {
	return p.Out("signatures")
}

// OutLog returns the Log out-port
func (p *CreateSparseTrain) OutLog() *sp.OutPort {
	return p.Out("log")
}
