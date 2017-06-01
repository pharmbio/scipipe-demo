package main

import (
	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
	"strconv"
	"strings"
)

const (
	// ------------------------------------------------
	// Set up paths
	// ------------------------------------------------
	tmpDir      = "tmp"
	dataDir     = "dat"
	appsDir     = dataDir + "/apps"
	refDir      = appsDir + "/pipeline_test/ref"
	origDataDir = appsDir + "/pipeline_test/data"
)

type DownloadWorkflow struct {
	Pipeline *sp.Pipeline
}

func NewDownloadWorkflow(tumorIndexes []string, normalIndexes []string) *DownloadWorkflow {
	pl := sp.NewPipeline()
	wf := &DownloadWorkflow{Pipeline: pl}

	pl.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	pl.GetProc("download_apps").SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	pl.NewProc("unzip_apps", "zcat {i:targz} > {o:tar}")
	pl.GetProc("unzip_apps").SetPathReplace("targz", "tar", ".gz", "")
	pl.Connect("unzip_apps.targz <- download_apps.apps")

	pl.NewProc("untar_apps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	pl.GetProc("untar_apps").SetPathStatic("outdir", dataDir+"/apps")
	pl.Connect("untar_apps.tar <- unzip_apps.tar")

	return wf
}

func (wf *DownloadWorkflow) Run() {
	wf.Pipeline.Run()
}

func main() {
	// Output slightly more info than default
	sp.InitLogInfo()

	// ----------------------------------------------------------------------------
	// Some general stuff used in multiple places below
	// ----------------------------------------------------------------------------

	indexes := map[string][]string{
		"normal": []string{"1", "2", "4", "7", "8"},
		"tumor":  []string{"1", "2", "3", "5", "6", "7"},
	}
	indexQueue := map[string]*ParamQueue{}

	sampleTypes := map[string][]string{
		"normal": []string{"normal", "normal", "normal", "normal", "normal"},
		"tumor":  []string{"tumor", "tumor", "tumor", "tumor", "tumor", "tumor"},
	}
	stQueue := map[string]*ParamQueue{}

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------

	downloadWf := NewDownloadWorkflow(indexes["tumor"], indexes["normal"])
	downloadWf.Run()

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------

	pr := sp.NewPipelineRunner()

	refFasta := refDir + "/human_g1k_v37_decoy.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta.fai"

	readsPaths1 := map[string][]string{}
	readsPaths2 := map[string][]string{}

	// Init some process "holders"
	alignSamples := map[string]*sp.SciProcess{}
	mergeBams := map[string]*sp.SciProcess{}
	markDupes := map[string]*sp.SciProcess{}

	readsFQ1 := map[string]*sp.IPQueue{}
	readsFQ2 := map[string]*sp.IPQueue{}
	streamToSubstream := map[string]*spcomp.StreamToSubStream{}

	// Init the main sink
	mainWfSink := sp.NewSink()

	for i, smpltype := range []string{"normal", "tumor"} {

		si := strconv.Itoa(i)

		indexQueue[smpltype] = NewParamQueue(indexes[smpltype]...)
		pr.AddProcess(indexQueue[smpltype])

		stQueue[smpltype] = NewParamQueue(sampleTypes[smpltype]...)
		pr.AddProcess(stQueue[smpltype])

		for _, idx := range indexes[smpltype] {
			readsPaths1[smpltype] = append(readsPaths1[smpltype], origDataDir+"/tiny_"+smpltype+"_L00"+idx+"_R1.fastq.gz")
			readsPaths2[smpltype] = append(readsPaths2[smpltype], origDataDir+"/tiny_"+smpltype+"_L00"+idx+"_R2.fastq.gz")
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		readsFQ1[smpltype] = sp.NewIPQueue(readsPaths1[smpltype]...)
		readsFQ2[smpltype] = sp.NewIPQueue(readsPaths2[smpltype]...)
		pr.AddProcess(readsFQ1[smpltype])
		pr.AddProcess(readsFQ2[smpltype])

		alignSamples[smpltype] = pr.NewFromShell("align_samples_"+smpltype, "bwa mem -R \"@RG\tID:{p:smpltyp}_{p:indexno}\tSM:{p:smpltyp}\tLB:{p:smpltyp}\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
			"| samtools view -bS -t "+refIndex+" - "+
			"| samtools sort - > {o:bam}")
		alignSamples[smpltype].In("reads_1").Connect(readsFQ1[smpltype].Out)
		alignSamples[smpltype].In("reads_2").Connect(readsFQ2[smpltype].Out)
		alignSamples[smpltype].PP("indexno").Connect(indexQueue[smpltype].Out)
		alignSamples[smpltype].PP("smpltyp").Connect(stQueue[smpltype].Out)
		alignSamples[smpltype].SetPathCustom("bam", func(t *sp.SciTask) string {
			outPath := tmpDir + "/" + t.Params["smpltyp"] + "_" + t.Params["indexno"] + ".bam"
			return outPath
		})

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream[smpltype] = spcomp.NewStreamToSubStream()
		streamToSubstream[smpltype].In.Connect(alignSamples[smpltype].Out("bam"))
		pr.AddProcess(streamToSubstream[smpltype])

		mergeBams[smpltype] = pr.NewFromShell("merge_bams_"+smpltype, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams[smpltype].In("bams").Connect(streamToSubstream[smpltype].OutSubStream)
		mergeBams[smpltype].SetPathStatic("mergedbam", tmpDir+"/"+smpltype+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		markDupes[smpltype] = pr.NewFromShell("mark_dupes_"+smpltype,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+smpltype+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+smpltype+`_`+si+`.md{.bam.tmp,}.bai;`)
		markDupes[smpltype].SetPathStatic("bam", tmpDir+"/"+smpltype+"_"+si+".md.bam")
		markDupes[smpltype].In("bam").Connect(mergeBams[smpltype].Out("mergedbam"))
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------

	markDupesNormalFanOut := spcomp.NewFanOut()
	markDupesNormalFanOut.InFile.Connect(markDupes["normal"].Out("bam"))
	pr.AddProcess(markDupesNormalFanOut)

	markDupesTumorFanOut := spcomp.NewFanOut()
	markDupesTumorFanOut.InFile.Connect(markDupes["tumor"].Out("bam"))
	pr.AddProcess(markDupesTumorFanOut)

	realignCreateTargets := pr.NewFromShell("realign_create_targets",
		`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T RealignerTargetCreator  \
				-I {i:bamnormal} \
				-I {i:bamtumor} \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-known `+refDir+`/1000G_phase1.indels.b37.vcf \
				-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
				-nt 4 \
				-XL hs37d5 \
				-XL NC_007605 \
				-o {o:intervals}`)
	realignCreateTargets.In("bamnormal").Connect(markDupesNormalFanOut.Out("create_targets"))
	realignCreateTargets.In("bamtumor").Connect(markDupesTumorFanOut.Out("create_targets"))
	realignCreateTargets.SetPathStatic("intervals", tmpDir+"/tiny.intervals")

	// --------------------------------------------------------------------------------
	// Re-align Reads - Re-align Indels
	// --------------------------------------------------------------------------------

	realignIndels := pr.NewFromShell("realign_indels",
		`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T IndelRealigner \
			-I {i:bamnormal} \
			-I {i:bamtumor} \
			-R `+refDir+`/human_g1k_v37_decoy.fasta \
			-targetIntervals {i:intervals} \
			-known `+refDir+`/1000G_phase1.indels.b37.vcf \
			-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
			-XL hs37d5 \
			-XL NC_007605 \
			-nWayOut '.real.bam.tmp' # {o:realbamnormal} {o:realbamtumor};
			realn={o:realbamnormal};
			realt={o:realbamtumor};
			mv $realn.bai ${realn%.bam.tmp}.bai;
			mv $realt.bai ${realt%.bam.tmp}.bai;`)
	realignIndels.In("intervals").Connect(realignCreateTargets.Out("intervals"))
	realignIndels.In("bamnormal").Connect(markDupesNormalFanOut.Out("realign_indels"))
	realignIndels.In("bamtumor").Connect(markDupesTumorFanOut.Out("realign_indels"))
	realignIndels.SetPathCustom("realbamnormal", func(t *sp.SciTask) string {
		path := t.InTargets["bamnormal"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	realignIndels.SetPathCustom("realbamtumor", func(t *sp.SciTask) string {
		path := t.InTargets["bamtumor"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})

	// --------------------------------------------------------------------------------
	// Re-calibrate reads
	// --------------------------------------------------------------------------------

	realBamFanOut := map[string]*spcomp.FanOut{}

	reCalibrate := map[string]*sp.SciProcess{}
	printReads := map[string]*sp.SciProcess{}

	for _, smpltype := range []string{"normal", "tumor"} {

		realBamFanOut[smpltype] = spcomp.NewFanOut()
		realBamFanOut[smpltype].InFile.Connect(realignIndels.Out("realbam" + smpltype))
		pr.AddProcess(realBamFanOut[smpltype])

		reCalibrate[smpltype] = pr.NewFromShell("recalibrate_"+smpltype,
			`java -Xmx3g -Djava.io.tmpdir=`+tmpDir+` -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T BaseRecalibrator \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-I {i:realbam} \
				-knownSites `+refDir+`/dbsnp_138.b37.vcf \
				-knownSites `+refDir+`/1000G_phase1.indels.b37.vcf \
				-knownSites `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
				-nct 4 \
				-XL hs37d5 \
				-XL NC_007605 \
				-l INFO \
				-o {o:recaltable}`)
		reCalibrate[smpltype].In("realbam").Connect(realBamFanOut[smpltype].Out("recal"))
		reCalibrate[smpltype].SetPathStatic("recaltable", tmpDir+"/"+smpltype+".recal.table")

		printReads[smpltype] = pr.NewFromShell("print_reads_"+smpltype,
			`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-nct 4 \
				-I {i:realbam} \
				-XL hs37d5 \
				-XL NC_007605 \
				--BQSR {i:recaltable} \
				-o {o:recalbam};
				fname={o:recalbam};
				mv $fname.bai ${fname%.bam.tmp}.bai;`)
		printReads[smpltype].In("realbam").Connect(realBamFanOut[smpltype].Out("printreads"))
		printReads[smpltype].In("recaltable").Connect(reCalibrate[smpltype].Out("recaltable"))
		printReads[smpltype].SetPathStatic("recalbam", smpltype+".recal.bam")

		mainWfSink.Connect(printReads[smpltype].Out("recalbam"))
	}

	pr.AddProcess(mainWfSink)
	pr.Run()
}

// ----------------------------------------------------------------------------
// Helper processes
// ----------------------------------------------------------------------------

type CombinationCreator struct {
}

func NewCombinationCreator(paramVals map[string][]string) *CombinationCreator {
	for _, _ = range paramVals {

	}
	return &CombinationCreator{}
}

// ----------------------------------------------------------------------------

type ParamQueue struct {
	sp.Process
	Out    *sp.ParamPort
	params []string
}

func NewParamQueue(params ...string) *ParamQueue {
	return &ParamQueue{
		Out:    sp.NewParamPort(),
		params: params,
	}
}

func (p *ParamQueue) Run() {
	defer p.Out.Close()
	for _, param := range p.params {
		p.Out.Chan <- param
	}
}

func (p *ParamQueue) IsConnected() bool {
	return p.Out.IsConnected()
}

// ----------------------------------------------------------------------------

type FileMultiplicator struct {
	sp.Process
	In                   *sp.FilePort
	Out                  *sp.FilePort
	multiplicationFactor int
}

func NewFileMultiplicator(multiplicationFactor int) *FileMultiplicator {
	return &FileMultiplicator{
		In:                   sp.NewFilePort(),
		Out:                  sp.NewFilePort(),
		multiplicationFactor: multiplicationFactor,
	}
}

func (p *FileMultiplicator) Run() {
	defer p.Out.Close()

	for inFile := range p.In.Chan {
		path := inFile.GetPath()
		for i := 0; i < p.multiplicationFactor; i++ {
			p.Out.Chan <- sp.NewInformationPacket(path)
		}
	}
}

func (p *FileMultiplicator) IsConnected() bool {
	return p.In.IsConnected() && p.Out.IsConnected()
}
