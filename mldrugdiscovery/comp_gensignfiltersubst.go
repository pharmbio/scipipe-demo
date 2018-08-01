package main

import (
	sp "github.com/scipipe/scipipe"
)

type GenSignFilterSubst struct {
	*sp.Process
}

type GenSignFilterSubstConf struct {
	replicateID string
	threadsCnt  int
	minHeight   int
	maxHeight   int
	slientMode  bool
}

// NewGenSignFilterSubst returns a new GenSignFilterSubstConf process
func NewGenSignFilterSubst(wf *sp.Workflow, name string, params GenSignFilterSubstConf) *GenSignFilterSubst {
	cmd := `java -jar ../bin/GenerateSignatures.jar \
		-inputfile {i:smiles} \
		-threads {p:threads} \
		-minheight {p:minheight} \
		-maxheight {p:maxheight} \
		-outputfile {o:signatures}`
	if params.slientMode {
		cmd += ` \
		-silent`
	}
	p := wf.NewProc(name, cmd)
	p.InParam("threads").FromInt(params.threadsCnt)
	p.InParam("minheight").FromInt(params.minHeight)
	p.InParam("maxheight").FromInt(params.maxHeight)
	p.SetOut("signatures", "{i:smiles}.{p:minheight}_{p:maxheight}.sign")
	return &GenSignFilterSubst{p}
}

// InSmiles takes input file in SMILES format
func (p *GenSignFilterSubst) InSmiles() *sp.InPort {
	return p.In("smiles")
}

// OutSignatures returns output files as text files with signatures
func (p *GenSignFilterSubst) OutSignatures() *sp.OutPort {
	return p.Out("signatures")
}
