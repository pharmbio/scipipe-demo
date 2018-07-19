package main

import sp "github.com/scipipe/scipipe"

// PredictLibLinear does blabla ...
type PredictLibLinear struct {
	*sp.Process
}

// PredictLibLinearConf contains parameters for initializing a
// PredictLibLinear process
type PredictLibLinearConf struct {
}

// NewPredictLibLinear returns a new PredictLibLinear process
func NewPredictLibLinear(wf *sp.Workflow, name string, params PredictLibLinearConf) *PredictLibLinear {
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("out", "out.txt")
	return &PredictLibLinear{p}
}

// InInfile returns the Infile in-port
func (p *PredictLibLinear) InInfile() *sp.InPort {
	return p.In("in")
}

// OutOutfile returns the Outfile out-port
func (p *PredictLibLinear) OutOutfile() *sp.OutPort {
	return p.Out("out")
}

//class PredictLinearModel(sl.Task):
//    # INPUT TARGETS
//    in_model = None
//    in_sparse_testdata = None
//
//    # TASK PARAMETERS
//    replicate_id = luigi.Parameter()
//
//    # DEFINE OUTPUTS
//    def out_prediction(self):
//        return sl.TargetInfo(self, self.in_model().path + '.pred')
//
//    # WHAT THE TASK DOES
//    def run(self):
//        self.ex(['bin/lin-predict',
//            self.in_sparse_testdata().path,
//            self.in_model().path,
//            self.out_prediction().path])
