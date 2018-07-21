package main

import (
	sp "github.com/scipipe/scipipe"
)

// TrainLibLinear does blabla ...
type TrainLibLinear struct {
	*sp.Process
}

// TrainLibLinearConf contains parameters for initializing a
// TrainLibLinear process
type TrainLibLinearConf struct {
	ReplicateID string
	Cost        float64
	SolverType  int
}

// NewTrainLibLinear returns a new TrainLibLinear process
func NewTrainLibLinear(wf *sp.Workflow, name string, params TrainLibLinearConf) *TrainLibLinear {
	cmd := `/usr/bin/time -f%e -o {o:traintime} ` +
		`../bin/lin-train -s {p:solvertype} -c {p:cost} -q {i:traindata} {o:model}`
	p := wf.NewProc(name, cmd)

	p.InParam("solvertype").FromInt(params.SolverType)
	if params.Cost != 0 {
		p.InParam("cost").FromFloat(params.Cost)
	}
	p.SetOut("model", fs("{i:traindata}.s%d_c{p:cost}.linmdl", params.SolverType))
	p.SetOut("traintime", "{o:model}.traintime")

	return &TrainLibLinear{p}
}

// InTrainData returns the TrainData in-port
func (p *TrainLibLinear) InTrainData() *sp.InPort {
	return p.In("traindata")
}

// OutModel returns the Model out-port
func (p *TrainLibLinear) OutModel() *sp.OutPort {
	return p.Out("model")
}
