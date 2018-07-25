package main

import (
	sp "github.com/scipipe/scipipe"
)

// CreateSparseTest does blabla ...
type CreateSparseTest struct {
	*sp.Process
}

// CreateSparseTestConf contains parameters for initializing a
// CreateSparseTest process
type CreateSparseTestConf struct {
	ReplicateID string
}

// NewCreateSparseTest returns a new CreateSparseTest process
func NewCreateSparseTest(wf *sp.Workflow, name string, params CreateSparseTestConf) *CreateSparseTest {
	cmd := `java -jar ../bin/CreateSparseDataset.jar \
	-inputfile {i:testdata} \
	-signaturesinfile {i:signaturesinfile} \
	-datasetfile {o:sparsetest} \
	-signaturesoutfile {o:signatures} \
	-silent`
	p := wf.NewProc(name, cmd)
	p.SetOut("sparsetest", "{i:testdata}.csr")
	p.SetOut("signatures", "{i:testdata}.sign")
	p.SetOut("log", "{i:testdata}.csr.log")
	return &CreateSparseTest{p}
}

// InTestdata returns the Testdata in-port
func (p *CreateSparseTest) InTestdata() *sp.InPort {
	return p.In("testdata")
}

// InSignatures returns the Testdata in-port
func (p *CreateSparseTest) InSignatures() *sp.InPort {
	return p.In("signaturesinfile")
}

// OutSparseTestdata returns the SparseTestdata out-port
func (p *CreateSparseTest) OutSparseTestdata() *sp.OutPort {
	return p.Out("sparsetest")
}

// OutSignatures returns the Signatures out-port
func (p *CreateSparseTest) OutSignatures() *sp.OutPort {
	return p.Out("signatures")
}

// OutLog returns the Log out-port
func (p *CreateSparseTest) OutLog() *sp.OutPort {
	return p.Out("log")
}
