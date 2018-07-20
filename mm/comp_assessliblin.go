package main

import (
	sp "github.com/scipipe/scipipe"
)

// AssessLibLinear does blabla ...
type AssessLibLinear struct {
	*sp.Process
}

// AssessLibLinearConf contains parameters for initializing a
// AssessLibLinear process
type AssessLibLinearConf struct {
	Cost float64
}

// NewAssessLibLinear returns a new AssessLibLinear process
func NewAssessLibLinear(wf *sp.Workflow, name string, params AssessLibLinearConf) *AssessLibLinear {
	cmd := `rmsd=$(awk 'FNR==NR { pred[FNR]=$1; next } ` +
		`{ sqdiffsum += (pred[FNR]-$1)^2; valcnt++ } ` +
		`END { rmsd=sqrt(sqdiffsum/valcnt); print rmsd }' ` +
		`{i:prediction} {i:testdata}) && ` + "\\\n" +
		fs(`echo "$rmsd	%f" > {o:rmsd_cost}`, params.Cost)
	p := wf.NewProc(name, cmd)
	p.SetOut("rmsd_cost", "{i:prediction}.rmsd_cost")
	return &AssessLibLinear{p}
}

// InTestData returns the TestData in-port
func (p *AssessLibLinear) InTestData() *sp.InPort {
	return p.In("testdata")
}

// InPrediction returns the Prediction in-port
func (p *AssessLibLinear) InPrediction() *sp.InPort {
	return p.In("prediction")
}

// OutRMSDCost returns the RMSDCost out-port
func (p *AssessLibLinear) OutRMSDCost() *sp.OutPort {
	return p.Out("rmsd_cost")
}
