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
	// def run(self):
	//     with self.in_sparse_testdata().open() as testfile:
	//         with self.in_prediction().open() as predfile:
	//             squared_diffs = []
	//             for tline, pline in zip(testfile, predfile):
	//                 test = float(tline.split(' ')[0])
	//                 pred = float(pline)
	//                 squared_diff = (pred-test)**2
	//                 squared_diffs.append(squared_diff)
	//     rmsd = math.sqrt(sum(squared_diffs)/len(squared_diffs))
	//     rmsd_records = {'rmsd': rmsd,
	//                     'cost': self.lin_cost}
	//     with self.out_assessment().open('w') as assessfile:
	//         sl.util.dict_to_recordfile(assessfile, rmsd_records)
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("assessment", "{i:prediction}.rmsd")
	return &AssessLibLinear{p}
}

// InModel returns the Model in-port
func (p *AssessLibLinear) InModel() *sp.InPort {
	return p.In("model")
}

// InTestData returns the TestData in-port
func (p *AssessLibLinear) InTestData() *sp.InPort {
	return p.In("testdata")
}

// InPrediction returns the Prediction in-port
func (p *AssessLibLinear) InPrediction() *sp.InPort {
	return p.In("prediction")
}

// OutAssessment returns the Assessment out-port
func (p *AssessLibLinear) OutAssessment() *sp.OutPort {
	return p.Out("assessment")
}
