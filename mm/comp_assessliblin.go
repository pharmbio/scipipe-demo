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
}

// NewAssessLibLinear returns a new AssessLibLinear process
func NewAssessLibLinear(wf *sp.Workflow, name string, params AssessLibLinearConf) *AssessLibLinear {
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("out", "out.txt")
	return &AssessLibLinear{p}
}

// InInfile returns the Infile in-port
func (p *AssessLibLinear) InInfile() *sp.InPort {
	return p.In("in")
}

// OutOutfile returns the Outfile out-port
func (p *AssessLibLinear) OutOutfile() *sp.OutPort {
	return p.Out("out")
}

//class AssessLinearRMSD(sl.Task): # TODO: Check with Jonalv whether RMSD is what we want to do?!!
//    # Parameters
//    lin_cost = luigi.Parameter()
//
//    # INPUT TARGETS
//    in_model = None
//    in_sparse_testdata = None
//    in_prediction = None
//
//    # DEFINE OUTPUTS
//    def out_assessment(self):
//        return sl.TargetInfo(self, self.in_prediction().path + '.rmsd')
//
//    # WHAT THE TASK DOES
//    def run(self):
//        with self.in_sparse_testdata().open() as testfile:
//            with self.in_prediction().open() as predfile:
//                squared_diffs = []
//                for tline, pline in zip(testfile, predfile):
//                    test = float(tline.split(' ')[0])
//                    pred = float(pline)
//                    squared_diff = (pred-test)**2
//                    squared_diffs.append(squared_diff)
//        rmsd = math.sqrt(sum(squared_diffs)/len(squared_diffs))
//        rmsd_records = {'rmsd': rmsd,
//                        'cost': self.lin_cost}
//        with self.out_assessment().open('w') as assessfile:
//            sl.util.dict_to_recordfile(assessfile, rmsd_records)
