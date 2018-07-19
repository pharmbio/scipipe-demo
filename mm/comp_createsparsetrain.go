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
}

// NewCreateSparseTrain returns a new CreateSparseTrain process
func NewCreateSparseTrain(wf *sp.Workflow, name string, params CreateSparseTrainConf) *CreateSparseTrain {
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("out", "out.txt")
	return &CreateSparseTrain{p}
}

// InInfile returns the Infile in-port
func (p *CreateSparseTrain) InInfile() *sp.InPort {
	return p.In("in")
}

// OutOutfile returns the Outfile out-port
func (p *CreateSparseTrain) OutOutfile() *sp.OutPort {
	return p.Out("out")
}

//class CreateSparseTrainDataset(sl.SlurmTask):
//
//    # TASK PARAMETERS
//    replicate_id = luigi.Parameter()
//
//    # INPUT TARGETS
//    in_traindata = None
//
//    def out_sparse_traindata(self):
//        return sl.TargetInfo(self, self.in_traindata().path + '.csr')
//
//    def out_signatures(self):
//        return sl.TargetInfo(self, self.in_traindata().path + '.signatures')
//
//    def out_log(self):
//        return sl.TargetInfo(self, self.in_traindata().path + '.csr.log')
//
//    # WHAT THE TASK DOES
//    def run(self):
//        self.ex(['java', '-jar', 'bin/CreateSparseDataset.jar',
//                '-inputfile', self.in_traindata().path,
//                '-datasetfile', self.out_sparse_traindata().path,
//                '-signaturesoutfile', self.out_signatures().path,
//                '-silent'])
