package main

import (
	sp "github.com/scipipe/scipipe"
)

// CreateFolds does blabla ...
type CreateFolds struct {
	*sp.Process
}

// CreateFoldsConf contains parameters for initializing a
// CreateFolds process
type CreateFoldsConf struct {
	FoldsCnt int
	FoldIdx  int
}

// NewCreateFolds returns a new CreateFolds process
func NewCreateFolds(wf *sp.Workflow, name string, params CreateFoldsConf) *CreateFolds {
	// Calculate start and end lines for extraction of the test data
	cmd := `linecnt=$(cat {i:linecnt}) && ` + "\\\n" +
		fs(`foldscnt=%d && `, params.FoldsCnt) + "\\\n" +
		fs(`foldidx=%d && `, params.FoldIdx) + "\\\n" +
		`linesperfold=$(echo "$linecnt / $foldscnt" | bc) && ` + "\\\n" +
		`tststart=$(echo "$foldidx * linesperfold" | bc) && ` + "\\\n" +
		`tstend=$(echo "($foldidx + 1) * linesperfold" | bc) && ` + "\\\n"

	// Create train dataset
	cmd += `awk -v tststart=$tststart -v tstend=$tstend 'NR < tststart || NR > tstend { print }' {i:in} > {o:traindata} && ` + "\\\n"

	// Create test dataset
	cmd += `awk -v tststart=$tststart -v tstend=$tstend 'NR >= tststart && NR <= tstend { print }' {i:in} > {o:testdata}`

	p := wf.NewProc(name, cmd)
	p.SetOut("traindata", fs("{i:in}.fld%02d_trn", params.FoldIdx))
	p.SetOut("testdata", fs("{i:in}.fld%02d_tst", params.FoldIdx))

	return &CreateFolds{p}
}

// InData returns the Data in-port
func (p *CreateFolds) InData() *sp.InPort {
	return p.In("in")
}

// InLineCnt returns the LineCnt in-port
func (p *CreateFolds) InLineCnt() *sp.InPort {
	return p.In("linecnt")
}

// OutTrainData returns the TrainData out-port
func (p *CreateFolds) OutTrainData() *sp.OutPort {
	return p.Out("traindata")
}

// OutTestData returns the TestData out-port
func (p *CreateFolds) OutTestData() *sp.OutPort {
	return p.Out("testdata")
}
