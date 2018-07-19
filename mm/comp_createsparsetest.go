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
}

// NewCreateSparseTest returns a new CreateSparseTest process
func NewCreateSparseTest(wf *sp.Workflow, name string, params CreateSparseTestConf) *CreateSparseTest {
	cmd := ``
	p := wf.NewProc(name, cmd)
	p.SetOut("out", "out.txt")
	return &CreateSparseTest{p}
}

// InInfile returns the Infile in-port
func (p *CreateSparseTest) InInfile() *sp.InPort {
	return p.In("in")
}

// OutOutfile returns the Outfile out-port
func (p *CreateSparseTest) OutOutfile() *sp.OutPort {
	return p.Out("out")
}

//class CreateSparseTestDataset(sl.Task):
//
//    # INPUT TARGETS
//    in_testdata = None
//    in_signatures = None
//
//    # TASK PARAMETERS
//    replicate_id = luigi.Parameter()
//    java_path = luigi.Parameter
//
//    # DEFINE OUTPUTS
//    def out_sparse_testdata(self):
//        return sl.TargetInfo(self, self.get_basepath()+ '.csr')
//    def out_signatures(self):
//        return sl.TargetInfo(self, self.get_basepath()+ '.signatures')
//    def out_log(self):
//        return sl.TargetInfo(self, self.get_basepath()+ '.csr.log')
//    def get_basepath(self):
//        return self.in_testdata().path
//
//    # WHAT THE TASK DOES
//    def run(self):
//        self.ex(['java', '-jar', 'bin/CreateSparseDataset.jar',
//                '-inputfile', self.in_testdata().path,
//                '-signaturesinfile', self.in_signatures().path,
//                '-datasetfile', self.out_sparse_testdata().path,
//                '-signaturesoutfile', self.out_signatures().path,
//                '-silent'])
