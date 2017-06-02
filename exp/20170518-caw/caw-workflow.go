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
	origDataDir = appsDir + "/pipeline_test/data"
	refDir      = appsDir + "/pipeline_test/ref"
	refFasta    = refDir + "/human_g1k_v37_decoy.fasta"
	refIndex    = refDir + "/human_g1k_v37_decoy.fasta.fai"
)

func main() {
	// Output slightly more info than default
	sp.InitLogInfo()

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------

	downloadWf := NewDownloadWorkflow(dataDir)
	downloadWf.Run()

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------

	pr := sp.NewPipelineRunner()
	mainWfSink := sp.NewSink()

	// Some parameter stuff used below

	sampleTypes := []string{"normal", "tumor"}

	indexes := map[string][]string{
		"normal": {"1", "2", "4", "7", "8"},
		"tumor":  {"1", "2", "3", "5", "6", "7"},
	}

	sampleTypeLists := map[string][]string{
		"normal": {"normal", "normal", "normal", "normal", "normal"},
		"tumor":  {"tumor", "tumor", "tumor", "tumor", "tumor", "tumor"},
	}

	// Init a process "holder" for the final process in this part, as we need
	// to access the normal and tumor verions specifically
	markDupesProcs := map[string]*sp.SciProcess{}

	for i, sampleType := range sampleTypes {
		si := strconv.Itoa(i)

		indexQueue := spcomp.NewStringGenerator(indexes[sampleType]...)
		pr.AddProcess(indexQueue)

		stQueue := spcomp.NewStringGenerator(sampleTypeLists[sampleType]...)
		pr.AddProcess(stQueue)

		readsPaths1 := []string{}
		readsPaths2 := []string{}
		for _, idx := range indexes[sampleType] {
			readsPaths1 = append(readsPaths1, origDataDir+"/tiny_"+sampleType+"_L00"+idx+"_R1.fastq.gz")
			readsPaths2 = append(readsPaths2, origDataDir+"/tiny_"+sampleType+"_L00"+idx+"_R2.fastq.gz")
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		readsFQ1 := sp.NewIPQueue(readsPaths1...)
		pr.AddProcess(readsFQ1)

		readsFQ2 := sp.NewIPQueue(readsPaths2...)
		pr.AddProcess(readsFQ2)

		alignSamples := NewBwaAlign("align_samples", sampleType, refFasta, refIndex)
		alignSamples.InReads1().Connect(readsFQ1.Out)
		alignSamples.InReads2().Connect(readsFQ2.Out)
		alignSamples.PPIndexNo().Connect(indexQueue.Out)
		alignSamples.PPSampleType().Connect(stQueue.Out)
		pr.AddProcess(alignSamples)

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream := spcomp.NewStreamToSubStream()
		streamToSubstream.In.Connect(alignSamples.OutBam())
		pr.AddProcess(streamToSubstream)

		mergeBams := pr.NewFromShell("merge_bams_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams.In("bams").Connect(streamToSubstream.OutSubStream)
		mergeBams.SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		markDupes := pr.NewFromShell("mark_dupes_"+sampleType,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+si+`.md{.bam.tmp,}.bai;`)
		markDupes.SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+si+".md.bam")
		markDupes.In("bam").Connect(mergeBams.Out("mergedbam"))

		markDupesProcs[sampleType] = markDupes
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------

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
	realignCreateTargets.In("bamnormal").Connect(markDupesProcs["normal"].Out("bam"))
	realignCreateTargets.In("bamtumor").Connect(markDupesProcs["tumor"].Out("bam"))
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
	realignIndels.In("bamnormal").Connect(markDupesProcs["normal"].Out("bam"))
	realignIndels.In("bamtumor").Connect(markDupesProcs["tumor"].Out("bam"))
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

	for _, sampleType := range sampleTypes {
		reCalibrate := pr.NewFromShell("recalibrate_"+sampleType,
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
		reCalibrate.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
		reCalibrate.SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")

		printReads := pr.NewFromShell("print_reads_"+sampleType,
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
		printReads.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
		printReads.In("recaltable").Connect(reCalibrate.Out("recaltable"))
		printReads.SetPathStatic("recalbam", sampleType+".recal.bam")

		mainWfSink.Connect(printReads.Out("recalbam"))
	}

	pr.AddProcess(mainWfSink)
	pr.Run()
}

// ----------------------------------------------------------------------------
// Sub-workflows
// ----------------------------------------------------------------------------

type DownloadWorkflow struct {
	*sp.Pipeline
}

func NewDownloadWorkflow(dataDir string) *DownloadWorkflow {
	wf := &DownloadWorkflow{sp.NewPipeline()}

	wf.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	wf.GetProc("download_apps").SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	wf.NewProc("unzip_apps", "zcat {i:targz} > {o:tar}")
	wf.GetProc("unzip_apps").SetPathReplace("targz", "tar", ".gz", "")
	wf.Connect("unzip_apps.targz <- download_apps.apps")

	wf.NewProc("untar_apps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	wf.GetProc("untar_apps").SetPathStatic("outdir", dataDir+"/apps")
	wf.Connect("untar_apps.tar <- unzip_apps.tar")

	return wf
}

// ----------------------------------------------------------------------------
// Component library
// ----------------------------------------------------------------------------

type BwaAlign struct {
	*sp.SciProcess
}

func NewBwaAlign(procIdPrefix string, sampleType string, refFasta string, refIndex string) *BwaAlign {
	innerBwaAlign := sp.NewFromShell(procIdPrefix+"_"+sampleType, "bwa mem -R \"@RG\tID:{p:smpltyp}_{p:indexno}\tSM:{p:smpltyp}\tLB:{p:smpltyp}\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
		"| samtools view -bS -t "+refIndex+" - "+
		"| samtools sort - > {o:bam}")
	innerBwaAlign.SetPathCustom("bam", func(t *sp.SciTask) string {
		outPath := tmpDir + "/" + t.Params["smpltyp"] + "_" + t.Params["indexno"] + ".bam"
		return outPath
	})
	return &BwaAlign{innerBwaAlign}
}

func (p *BwaAlign) PPIndexNo() *sp.ParamPort    { return p.PP("indexno") }
func (p *BwaAlign) PPSampleType() *sp.ParamPort { return p.PP("smpltyp") }
func (p *BwaAlign) InReads1() *sp.FilePort      { return p.In("reads_1") }
func (p *BwaAlign) InReads2() *sp.FilePort      { return p.In("reads_2") }
func (p *BwaAlign) OutBam() *sp.FilePort        { return p.Out("bam") }
