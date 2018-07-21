package main

// --------------------------------------------------------------------------------
// Reproduce SciPipe Case Study Workflow
// --------------------------------------------------------------------------------
// The code for a virtual machine with the SciLuigi version of this workflow is
// available [here](https://github.com/pharmbio/bioimg-sciluigi-casestudy), and
// the direct link to the code for this notebook is available
// [here](https://github.com/pharmbio/bioimg-sciluigi-casestudy/blob/master/roles/sciluigi_usecase/files/proj/largescale_svm/wffindcost.ipynb).
// --------------------------------------------------------------------------------

import (
	"path/filepath"
	"strings"

	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
)

const (
	dataDir = "data/"
)

func main() {
	dlWf := sp.NewWorkflow("download_jars", 2)
	downloadJars := dlWf.NewProc("download_jars", "wget https://ndownloader.figshare.com/files/6330402 -O {o:tarball}")
	downloadJars.SetOut("tarball", "jars.tar.gz")
	unpackJars := dlWf.NewProc("unpack_jars", "mkdir {o:unpackdir} && tar -zxf {i:tarball} -C {o:unpackdir}")
	unpackJars.SetOut("unpackdir", "bin")
	unpackJars.In("tarball").From(downloadJars.Out("tarball"))
	downloadRawData := dlWf.NewProc("download_rawdata", "wget https://raw.githubusercontent.com/pharmbio/bioimg-sciluigi-casestudy/master/roles/sciluigi_usecase/files/proj/largescale_svm/data/testrun_dataset.smi -O {o:dataset}")
	downloadRawData.SetOut("dataset", dataDir+"testdataset.smi")
	dlWf.Run()

	crossValWF := NewCrossValidateWorkflow(4, CrossValidateWorkflowParams{
		DatasetName:      "testdataset",
		RunID:            "testrun",
		ReplicateID:      "r1",
		FoldsCount:       10,
		MinHeight:        1,
		MaxHeight:        3,
		TestSize:         1000,
		TrainSizes:       []int{500, 1000, 2000, 4000, 8000},
		CostVals:         []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5},
		SolverType:       12,
		RandomDataSizeMB: 10,
		Runmode:          RunModeLocal,
		SlurmProject:     "N/A",
	})
	crossValWF.PlotConf.EdgeLabels = false
	crossValWF.PlotGraph("mmdag.dot")
	crossValWF.Run()
}

// CrossValidateWorkflow finds the optimal SVM cost values via a grid-search,
// with cross-validation
type CrossValidateWorkflow struct {
	*sp.Workflow
}

// CrossValidateWorkflowParams is a container for parameters to
// CrossValidateWorkflow workflows
type CrossValidateWorkflowParams struct {
	DatasetName      string
	RunID            string
	ReplicateID      string
	ReplicateIDs     []string
	FoldsCount       int
	MinHeight        int
	MaxHeight        int
	TestSize         int
	TrainSizes       []int
	CostVals         []float64
	SolverType       int
	RandomDataSizeMB int
	Runmode          RunMode
	SlurmProject     string
}

// ================================================================================
// Start: Main Workflow definition
// ================================================================================

// NewCrossValidateWorkflow returns an initialized CrossValidateWorkflow
func NewCrossValidateWorkflow(maxTasks int, params CrossValidateWorkflowParams) *CrossValidateWorkflow {
	wf := sp.NewWorkflow("cross_validate", maxTasks)

	mmTestData := spcomp.NewFileSource(
		wf,
		"mmTestData",
		fs("data/%s.smi", params.DatasetName))

	//procs := []sp.WorkflowProcess{}
	//lowestRMSDs := []float64{}
	//mainWFRunners := []*sp.Workflow{}

	replicateIds := params.ReplicateIDs
	if params.ReplicateID != "" {
		replicateIds = []string{params.ReplicateID}
	}

	for _, replID := range replicateIds {
		replID := replID // Create local copy of variable to avoid access to global loop variable from closures
		uniq_r := fs("_%s", replID)

		// ------------------------------------------------------------------------
		// Generate signatures and filter substances
		// ------------------------------------------------------------------------
		genSign := NewGenSignFilterSubst(wf, "gensign"+uniq_r,
			GenSignFilterSubstConf{
				replicateID: replID,
				threadsCnt:  8,
				minHeight:   params.MinHeight,
				maxHeight:   params.MaxHeight,
			})
		genSign.InSmiles().From(mmTestData.Out())

		// ------------------------------------------------------------------------
		// Create a unique copy per run
		// ------------------------------------------------------------------------
		createRunCopy := wf.NewProc("create_runcopy"+uniq_r, "cp {i:orig} {o:copy} # {p:runid}")
		createRunCopy.SetOut("copy", fs("%s/{i:orig}", params.RunID))
		createRunCopy.SetOutFunc("copy", func(t *sp.Task) string {
			origPath := t.InPath("orig")
			return filepath.Dir(origPath) + "/" + t.Param("runid") + "/" + filepath.Base(origPath)
		})
		createRunCopy.InParam("runid").FromStr(params.RunID)
		createRunCopy.In("orig").From(genSign.OutSignatures())

		// ------------------------------------------------------------------------
		// Create a unique copy per replicate
		// ------------------------------------------------------------------------
		createReplCopy := wf.NewProc("create_replcopy_"+uniq_r, "cp {i:orig} {o:copy} # {p:replid}")
		createReplCopy.SetOutFunc("copy", func(t *sp.Task) string {
			origPath := t.InPath("orig")
			return filepath.Dir(origPath) + "/" + t.Param("replid") + "/" + filepath.Base(origPath)
		})
		createReplCopy.InParam("replid").FromStr(replID)
		createReplCopy.In("orig").From(genSign.OutSignatures())

		for _, trainSize := range params.TrainSizes {
			uniq_rt := uniq_r + fs("_tr%d", trainSize)
			selBestCostPerTrainSizeSubstr := spcomp.NewStreamToSubStream(wf, "selbestcostpertrainsize"+uniq_rt)
			// ------------------------------------------------------------------------
			// Sample train and test
			// ------------------------------------------------------------------------
			sampleTrainTest := NewSampleTrainAndTest(wf, "sample_train_test"+uniq_rt,
				SampleTrainAndTestConf{
					ReplicateID:    replID,
					SamplingMethod: SamplingMethodRandom,
					TrainSize:      trainSize,
					TestSize:       params.TestSize,
				})
			sampleTrainTest.InSignatures().From(createReplCopy.Out("copy"))

			// ------------------------------------------------------------------------
			// Create sparse train dataset
			// ------------------------------------------------------------------------
			sparseTrain := NewCreateSparseTrain(wf, "sparsetrain"+uniq_rt, CreateSparseTrainConf{
				ReplicateID: replID,
			})
			sparseTrain.InTraindata().From(sampleTrainTest.OutTraindata())
			// Ad-hoc process to un-gzip the sparse train data file
			gunzipSparseTrain := wf.NewProc("gunzip_sparsetrain"+uniq_rt, "zcat {i:orig} > {o:ungzipped}")
			gunzipSparseTrain.In("orig").From(sparseTrain.OutSparseTraindata())
			gunzipSparseTrain.SetOut("ungzipped", "{i:orig}.ungz")

			// ------------------------------------------------------------------------
			// Create sparse test dataset
			// ------------------------------------------------------------------------
			sparseTest := NewCreateSparseTest(wf, "sparsetest"+uniq_rt, CreateSparseTestConf{
				ReplicateID: replID,
			})
			sparseTest.InTestdata().From(sampleTrainTest.OutTraindata())
			sparseTest.InSignatures().From(sparseTrain.OutSignatures())
			// Ad-hoc process to un-gzip the sparse train data file
			gunzipSparseTest := wf.NewProc("gunzip_sparsetest"+uniq_rt, "zcat {i:orig} > {o:ungzipped}")
			gunzipSparseTest.In("orig").From(sparseTest.OutSparseTestdata())
			gunzipSparseTest.SetOut("ungzipped", "{i:orig}.ungz")

			// ------------------------------------------------------------------------
			// Count train data
			// ------------------------------------------------------------------------
			cntTrainData := NewCountLines(wf, "cnttrain"+uniq_rt, CountLinesConf{})
			cntTrainData.InFile().From(gunzipSparseTrain.Out("ungzipped"))

			// ------------------------------------------------------------------------
			// Generate random data
			// ------------------------------------------------------------------------
			genRandBytes := NewGenRandBytes(wf, "genrand"+uniq_rt,
				GenRandBytesConf{
					SizeMB:      params.RandomDataSizeMB,
					ReplicateID: replID,
				})
			genRandBytes.InBasePath().From(gunzipSparseTrain.Out("ungzipped"))

			// ------------------------------------------------------------------------
			// Shuffle train data
			// ------------------------------------------------------------------------
			shufTrain := NewShuffleLines(wf, fs("shuftrain_%d_%s", trainSize, replID), ShuffleLinesConf{})
			shufTrain.InData().From(gunzipSparseTrain.Out("ungzipped"))
			shufTrain.InRandBytes().From(genRandBytes.OutRandBytes())

			// ------------------------------------------------------------------------
			// Loop over folds
			// ------------------------------------------------------------------------
			for _, cost := range params.CostVals {
				uniq_rtc := uniq_rt + fs("_c%f", cost)
				avgRMSDPerCostSubstr := spcomp.NewStreamToSubStream(wf, "cost_substr"+uniq_rtc)

				for foldIdx := 1; foldIdx <= params.FoldsCount; foldIdx++ {
					uniq_rtcf := uniq_rtc + fs("_fld%d", foldIdx)
					createFolds := NewCreateFolds(wf, "createfolds_"+uniq_rtcf,
						CreateFoldsConf{
							FoldIdx:  foldIdx,
							FoldsCnt: params.FoldsCount,
							// Seed?
						})
					createFolds.InData().From(shufTrain.OutShuffled())
					createFolds.InLineCnt().From(cntTrainData.OutLineCount())

					// ----------------------------------------------------------------
					// Train
					// ----------------------------------------------------------------
					trainLibLin := NewTrainLibLinear(wf, "train"+uniq_rtcf,
						TrainLibLinearConf{
							ReplicateID: replID,
							Cost:        cost,
							SolverType:  params.SolverType,
						})
					trainLibLin.InTrainData().From(createFolds.OutTrainData())

					// ----------------------------------------------------------------
					// Predict
					// ----------------------------------------------------------------
					predLibLin := NewPredictLibLinear(wf, "pred"+uniq_rtcf,
						PredictLibLinearConf{
							ReplicateID: replID,
						})
					predLibLin.InModel().From(trainLibLin.OutModel())
					predLibLin.InTestData().From(createFolds.OutTestData())

					// ----------------------------------------------------------------
					// Assess
					// ----------------------------------------------------------------
					assessLibLin := NewAssessLibLinear(wf, "assess"+uniq_rtcf, AssessLibLinearConf{})
					assessLibLin.InTestData().From(createFolds.OutTestData())
					assessLibLin.InPrediction().From(predLibLin.OutPrediction())
					assessLibLin.InParamCost().FromFloat(cost)

					avgRMSDPerCostSubstr.In().From(assessLibLin.OutRMSDCost())
				}

				avgRMSD := wf.NewProc("avg_rmsd"+uniq_rtc, `cat {i:rmsdcost|join: } | awk '{ c += $1; n++ } END { print c / n "\t" {p:cost} }' > {o:avgrmsd}`)
				avgRMSD.SetOut("avgrmsd", "data/avg_rmsd/avg_rmsd"+uniq_rtc+".txt")
				avgRMSD.InParam("cost").FromFloat(cost)
				avgRMSD.In("rmsdcost").From(avgRMSDPerCostSubstr.OutSubStream())

				selBestCostPerTrainSizeSubstr.In().From(avgRMSD.Out("avgrmsd"))
			} // end for cost

			// ----------------------------------------------------------------
			// Select best cost
			// ----------------------------------------------------------------
			selBestCostPerTrainSize := wf.NewProc("selbestcost"+uniq_rt, `cat {i:rmsdcost|join: } | awk 'BEGIN { rmsd = 1 } ($1 < rmsd) { rmsd = $1; cost = $2 } END { print {p:trainsize} "\t" rmsd "\t" cost }' > {o:bestcost}`)
			selBestCostPerTrainSize.InParam("trainsize").FromInt(trainSize)
			selBestCostPerTrainSize.SetOut("bestcost", "data/best_cost/"+fs("trainsize_%d", trainSize)+"/best_cost"+uniq_rt+".txt")
			selBestCostPerTrainSize.In("rmsdcost").From(selBestCostPerTrainSizeSubstr.OutSubStream())

			costFileToParam := wf.NewProc("cost_filetoparam"+uniq_rt, "# {i:costfile}")
			costFileToParam.InitOutParamPort(costFileToParam, "costparam")
			costFileToParam.CustomExecute = func(t *sp.Task) {
				fileBytes := t.InIP("costfile").Read()
				fileStr := strings.Trim(string(fileBytes), " \n")
				parts := strings.Split(fileStr, "	")
				cost := parts[2]
				t.Process.OutParam("costparam").Send(cost)
			}
			costFileToParam.In("costfile").From(selBestCostPerTrainSize.Out("bestcost"))

			// --------------------------------------------------------------------------------
			// Main training and assessment
			// --------------------------------------------------------------------------------
			// Train
			trainLibLin := NewTrainLibLinear(wf, "train_final"+uniq_rt,
				TrainLibLinearConf{
					ReplicateID: replID,
					SolverType:  params.SolverType,
				})
			trainLibLin.SetOut("model", fs("data/final_models/finalmodel"+uniq_rt+".s%d_c{p:cost}.linmdl", params.SolverType))
			trainLibLin.InTrainData().From(gunzipSparseTrain.Out("ungzipped"))
			trainLibLin.InParam("cost").From(costFileToParam.OutParam("costparam"))

			// Predict
			predLibLin := NewPredictLibLinear(wf, "pred_final"+uniq_rt,
				PredictLibLinearConf{
					ReplicateID: replID,
				})
			predLibLin.InModel().From(trainLibLin.OutModel())
			predLibLin.InTestData().From(gunzipSparseTest.Out("ungzipped"))

			// Assess
			assessLibLin := NewAssessLibLinear(wf, "assess_final"+uniq_rt,
				AssessLibLinearConf{})
			assessLibLin.InTestData().From(gunzipSparseTest.Out("ungzipped"))
			assessLibLin.InPrediction().From(predLibLin.OutPrediction())
			assessLibLin.InParam("cost").From(costFileToParam.OutParam("costparam"))

		} // end for train size
	} // end for replicate id
	return &CrossValidateWorkflow{wf}
}

// ================================================================================
// End: Main Workflow definition
// ================================================================================

//        mergedreport = self.new_task('merged_report_%s_%s' % (self.dataset_name, self.run_id), MergedDataReport,
//                run_id = self.run_id)
//        mergedreport.in_reports = [t.out_report for t in mainwfruns]
//
//        return mergedreport
