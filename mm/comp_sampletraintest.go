package main

import (
	"fmt"

	sp "github.com/scipipe/scipipe"
)

// SampleTrainAndTest samples train and test datasets from an input dataset
// consisting of a text file with row-wise values.
type SampleTrainAndTest struct {
	*sp.Process
}

type SamplingMethod string

const (
	SamplingMethodSignCnt SamplingMethod = "signcnt"
	SamplingMethodRandom  SamplingMethod = "rand"
)

// SampleTrainAndTestConf contains parameters for initializing a
// SampleTrainAndTest process
type SampleTrainAndTestConf struct {
	ReplicateID    string
	TestSize       int
	TrainSize      int
	Seed           int
	SamplingMethod SamplingMethod
}

// NewSampleTrainAndTest return a new SampleTrainAndTestConf process
func NewSampleTrainAndTest(wf *sp.Workflow, name string, params SampleTrainAndTestConf) *SampleTrainAndTest {
	jarFile := map[SamplingMethod]string{
		SamplingMethodRandom:  "SampleTrainingAndTest",
		SamplingMethodSignCnt: "SampleTrainingAndTestSizeBased",
	}[params.SamplingMethod]

	cmd := fmt.Sprintf(`java -jar ../bin/%s.jar \
		-inputfile {i:signatures} \
		-testfile {o:testdata} \
		-trainingfile {o:traindata} \
		-testsize %d \
		-trainingsize %d \
		-silent`,
		jarFile,
		params.TestSize,
		params.TrainSize)
	if params.Seed != 0 {
		cmd += fmt.Sprintf(` \
		-seed %d`, params.Seed)
	}

	p := wf.NewProc(name, cmd)
	fmtBasePath := func(t *sp.Task) string {
		return t.InPath("signatures") + fmt.Sprintf(".%d_%d_%s", params.TestSize, params.TrainSize, params.SamplingMethod)
	}
	p.SetOutFunc("traindata", func(t *sp.Task) string {
		return fmtBasePath(t) + "_trn"
	})
	p.SetOutFunc("testdata", func(t *sp.Task) string {
		return fmtBasePath(t) + "_tst"
	})
	p.SetOutFunc("log", func(t *sp.Task) string {
		return fmtBasePath(t) + "_trn.log"
	})
	return &SampleTrainAndTest{p}
}

// InSignatures returns the Signatures in-port
func (p *SampleTrainAndTest) InSignatures() *sp.InPort {
	return p.In("signatures")
}

// OutTraindata returns the Traindata out-port
func (p *SampleTrainAndTest) OutTraindata() *sp.OutPort {
	return p.Out("traindata")
}

// OutTestdata returns the Traindata out-port
func (p *SampleTrainAndTest) OutTestdata() *sp.OutPort {
	return p.Out("testdata")
}

// OutLog returns the Log out-port
func (p *SampleTrainAndTest) OutLog() *sp.OutPort {
	return p.Out("log")
}
