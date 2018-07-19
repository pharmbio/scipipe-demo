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
	crossValWF.PlotGraphPDF("mmdag.dot")
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

		// ------------------------------------------------------------------------
		// Generate signatures and filter substances
		// ------------------------------------------------------------------------
		genSign := NewGenSignFilterSubst(wf, fs("gensign_%s", replID),
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
		createRunCopy := wf.NewProc("create_runcopy_"+params.RunID, "cp {i:orig} {o:copy} # {p:runid}")
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
		createReplCopy := wf.NewProc("create_replcopy_"+replID, "cp {i:orig} {o:copy} # {p:replid}")
		createReplCopy.SetOutFunc("copy", func(t *sp.Task) string {
			origPath := t.InPath("orig")
			return filepath.Dir(origPath) + "/" + t.Param("replid") + "/" + filepath.Base(origPath)
		})
		createReplCopy.InParam("replid").FromStr(replID)
		createReplCopy.In("orig").From(genSign.OutSignatures())

		for _, trainSize := range params.TrainSizes {
			// ------------------------------------------------------------------------
			// Sample train and test
			// ------------------------------------------------------------------------
			sampleTrainTest := NewSampleTrainAndTest(wf, fs("sample_train_test_%d_%s", trainSize, replID),
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
			sparseTrain := NewCreateSparseTrain(wf, fs("sparsetrain_%d_%s", trainSize, replID), CreateSparseTrainConf{
				ReplicateID: replID,
			})
			sparseTrain.InTraindata().From(sampleTrainTest.OutTraindata())
			// Ad-hoc process to un-gzip the sparse train data file
			gunzipSparseTrain := wf.NewProc(fs("gunzip_sparsetrain_%d_%s", trainSize, replID), "zcat {i:orig} > {o:ungzipped}")
			gunzipSparseTrain.In("orig").From(sparseTrain.OutSparseTraindata())
			gunzipSparseTrain.SetOut("ungzipped", "{i:orig}.ungz")

			// ------------------------------------------------------------------------
			// Count train data
			// ------------------------------------------------------------------------
			cntTrainData := NewCountLines(wf, fs("cnttrain_%d_%s", trainSize, replID), CountLinesConf{})
			cntTrainData.InFile().From(gunzipSparseTrain.Out("ungzipped"))

			// ------------------------------------------------------------------------
			// Generate random data
			// ------------------------------------------------------------------------
			genRandBytes := NewGenRandBytes(wf, fs("genrandbytes_%d_%s", trainSize, replID),
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
			for foldIdx := 1; foldIdx <= params.FoldsCount; foldIdx++ {
				createFolds := NewCreateFolds(wf, fs("create_fold%02d_%d_%s", foldIdx, trainSize, replID),
					CreateFoldsConf{
						FoldIdx:  foldIdx,
						FoldsCnt: params.FoldsCount,
						// Seed?
					})
				createFolds.InData().From(shufTrain.OutShuffled())
				createFolds.InLineCnt().From(cntTrainData.OutLineCount())

				for _, cost := range params.CostVals {
					trainLibLin := NewTrainLibLinear(wf, fs("trainlin_%02d_%f_%d_%s", foldIdx, cost, trainSize, replID),
						TrainLibLinearConf{
							ReplicateID: replID,
							Cost:        cost,
							SolverType:  params.SolverType,
						})
					trainLibLin.InTrainData().From(createFolds.OutTrainData())

					predLibLin := NewPredictLibLinear(wf, fs("predict_fld%02d_%f_%d_%s", foldIdx, cost, trainSize, replID),
						PredictLibLinearConf{
							ReplicateID: replID,
						})
					predLibLin.InModel().From(trainLibLin.OutModel())
					predLibLin.InTestData().From(createFolds.OutTestData())

					// assess_lin = self.new_task('assesslin_fold_%d_cost_%s_%s_%s' % (fold_idx, cost, train_size, replicate_id), AssessLinearRMSD,
					//         lin_cost = cost,
					// assess_lin.in_model = train_lin.out_model
					// assess_lin.in_sparse_testdata = create_folds.out_testdata
					// assess_lin.in_prediction = pred_lin.out_prediction
				}
			}
		}
	}
	return &CrossValidateWorkflow{wf}
}

// ================================================================================
// End: Main Workflow definition
// ================================================================================

//                    for cost in costseq:
//                        tasks[replicate_id][fold_idx][cost] = {}

//                        tasks[replicate_id][fold_idx][cost] = {}
//                        tasks[replicate_id][fold_idx][cost]['create_folds'] = create_folds
//                        tasks[replicate_id][fold_idx][cost]['train_linear'] = train_lin
//                        tasks[replicate_id][fold_idx][cost]['predict_linear'] = pred_lin
//                        tasks[replicate_id][fold_idx][cost]['assess_linear'] = assess_lin
//
//                # Tasks for calculating average RMSD and finding the cost with lowest RMSD
//                avgrmsd_tasks = {}
//                for cost in costseq:
//                    # Calculate the average RMSD for each cost value
//                    average_rmsd = self.new_task('average_rmsd_cost_%s_%s_%s' % (cost, train_size, replicate_id), CalcAverageRMSDForCost,
//                            lin_cost=cost)
//                    average_rmsd.in_assessments = [tasks[replicate_id][fold_idx][cost]['assess_linear'].out_assessment for fold_idx in xrange(self.folds_count)]
//                    avgrmsd_tasks[cost] = average_rmsd

//                sel_lowest_rmsd = self.new_task('select_lowest_rmsd_%s_%s' % (train_size, replicate_id), SelectLowestRMSD)
//                sel_lowest_rmsd.in_values = [average_rmsd.out_rmsdavg for average_rmsd in avgrmsd_tasks.values()]

//                run_id = 'mainwfrun_liblinear_%s_tst%s_trn%s_%s' % (self.dataset_name, self.test_size, train_size, replicate_id)
//                mainwfrun = self.new_task('mainwfrun_%s_%s' % (train_size, replicate_id), MainWorkflowRunner,
//                        dataset_name=self.dataset_name,
//                        run_id=run_id,
//                        replicate_id=replicate_id,
//                        sampling_method='random',
//                        train_method='liblinear',
//                        train_size=train_size,
//                        test_size=self.test_size,
//                        lin_type=self.lin_type,
//                        slurm_project=self.slurm_project,
//                        parallel_lin_train=False,
//                        runmode=self.runmode)
//                mainwfrun.in_lowestrmsd = sel_lowest_rmsd.out_lowest

//                # Collect one lowest rmsd per train size
//                lowest_rmsds.append(sel_lowest_rmsd)
//
//                mainwfruns.append(mainwfrun)
//

//        mergedreport = self.new_task('merged_report_%s_%s' % (self.dataset_name, self.run_id), MergedDataReport,
//                run_id = self.run_id)
//        mergedreport.in_reports = [t.out_report for t in mainwfruns]
//
//        return mergedreport

//class MainWorkflowRunner(sciluigi.Task):
//    # Parameters
//    dataset_name = luigi.Parameter()
//    run_id = luigi.Parameter()
//    replicate_id =luigi.Parameter()
//    sampling_method = luigi.Parameter()
//    train_method = luigi.Parameter()
//    train_size = luigi.Parameter()
//    test_size = luigi.Parameter()
//    lin_type = luigi.Parameter()
//    slurm_project = luigi.Parameter()
//    parallel_lin_train = luigi.BoolParameter()
//    runmode = luigi.Parameter()
//
//    # In-ports (defined as fields accepting sciluigi.TargetInfo objects)
//    in_lowestrmsd = None
//
//    # Out-ports
//    def out_done(self):
//        return sciluigi.TargetInfo(self, self.in_lowestrmsd().path + '.mainwf_done')
//    def out_report(self):
//        outf_path = 'data/' + self.run_id + '/testrun_dataset_liblinear_datareport.csv'
//        return sciluigi.TargetInfo(self, outf_path) # We manually re-create the filename that this should have
//
//    # Task implementation
//    def run(self):
//        with self.in_lowestrmsd().open() as infile:
//            records = sciluigi.recordfile_to_dict(infile)
//            lowest_cost = records['lowest_cost']
//        self.ex('python wfmm.py' +
//                ' --dataset-name=%s' % self.dataset_name +
//                ' --run-id=%s' % self.run_id +
//                ' --replicate-id=%s' % self.replicate_id +
//                ' --sampling-method=%s' % self.sampling_method +
//                ' --train-method=%s' % self.train_method +
//                ' --train-size=%s' % self.train_size +
//                ' --test-size=%s' % self.test_size +
//                ' --lin-type=%s' % self.lin_type +
//                ' --lin-cost=%s' % lowest_cost +
//                ' --slurm-project=%s' % self.slurm_project +
//                ' --runmode=%s' % self.runmode)
//        with self.out_done().open('w') as donefile:
//            donefile.write('Done!\n')
