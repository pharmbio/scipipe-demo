package main

import sp "github.com/scipipe/scipipe"

// PredictLibLinear does blabla ...
type PredictLibLinear struct {
	*sp.Process
}

// PredictLibLinearConf contains parameters for initializing a
// PredictLibLinear process
type PredictLibLinearConf struct {
	ReplicateID string
}

// NewPredictLibLinear returns a new PredictLibLinear process
func NewPredictLibLinear(wf *sp.Workflow, name string, params PredictLibLinearConf) *PredictLibLinear {
	cmd := `../bin/lin-predict ` +
		`{i:testdata} ` +
		`{i:model} ` +
		`{o:prediction} `
	p := wf.NewProc(name, cmd)
	p.SetOut("prediction", "{i:model}.pred")

	return &PredictLibLinear{p}
}

// InModel returns the Model in-port
func (p *PredictLibLinear) InModel() *sp.InPort {
	return p.In("model")
}

// InTestData returns the TestData in-port
func (p *PredictLibLinear) InTestData() *sp.InPort {
	return p.In("testdata")
}

// OutPrediction returns the Prediction out-port
func (p *PredictLibLinear) OutPrediction() *sp.OutPort {
	return p.Out("prediction")
}
