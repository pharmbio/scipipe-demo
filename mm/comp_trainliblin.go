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
}

// NewTrainLibLinear returns a new TrainLibLinear process
func NewTrainLibLinear(wf *sp.Workflow, name string, params TrainLibLinearConf) *TrainLibLinear {
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("out", "out.txt")
	return &TrainLibLinear{p}
}

// InInfile returns the Infile in-port
func (p *TrainLibLinear) InInfile() *sp.InPort {
	return p.In("in")
}

// OutOutfile returns the Outfile out-port
func (p *TrainLibLinear) OutOutfile() *sp.OutPort {
	return p.Out("out")
}

//class TrainLinearModel(sl.SlurmTask):
//    # INPUT TARGETS
//    in_traindata = None
//
//    # TASK PARAMETERS
//    replicate_id = luigi.Parameter()
//    lin_type = luigi.Parameter() # 0 (regression)
//    lin_cost = luigi.Parameter() # 100
//    # Let's wait with implementing these
//    #lin_epsilon = luigi.Parameter()
//    #lin_bias = luigi.Parameter()
//    #lin_weight = luigi.Parameter()
//    #lin_folds = luigi.Parameter()
//
//    # Whether to run normal or distributed lib linear
//    #parallel_train = luigi.BooleanParameter()
//
//    # DEFINE OUTPUTS
//    def out_model(self):
//        return sl.TargetInfo(self, self.in_traindata().path + '.s{s}_c{c}.linmdl'.format(
//            s = self.lin_type,
//            c = self.lin_cost))
//
//    def out_traintime(self):
//        return sl.TargetInfo(self, self.out_model().path + '.extime')
//
//    # WHAT THE TASK DOES
//    def run(self):
//        #self.ex(['distlin-train',
//        self.ex(['/usr/bin/time', '-f%e', '-o',
//            self.out_traintime().path,
//            'bin/lin-train',
//            '-s', self.lin_type,
//            '-c', self.lin_cost,
//            '-q', # quiet mode
//            self.in_traindata().path,
//            self.out_model().path])
