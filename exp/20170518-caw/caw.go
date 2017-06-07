package main

import (
	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
	"strconv"
	"strings"
)

// Set up some paths

const (
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

	// Run the Data Download part of the workflow
	downloadDataWorkflow := NewDownloadWorkflow(dataDir)
	downloadDataWorkflow.Run()

	// ------------------------------------------------
	// Main workflow starts here
	// ------------------------------------------------

	// Some technical initialization
	pr := sp.NewPipelineRunner()
	mainWfSink := sp.NewSink()

	// Some parameter stuff used below
	sampleTypes := []string{"normal", "tumor"}
	readsIndexes := map[string][]string{
		"normal": {"1", "2", "4", "7", "8"},
		"tumor":  {"1", "2", "3", "5", "6", "7"},
	}

	// Init a process "holder" for the final process in this part, as we need
	// to access the normal and tumor verions specifically
	markDupesProcs := map[string]*GATKMarkDuplicates{}

	for sampleIdxStr, sampleType := range sampleTypes {
		sampleIdx := strconv.Itoa(sampleIdxStr)

		// Some parameter book-keeping
		indexQueue := spcomp.NewStringGenerator(readsIndexes[sampleType]...)
		pr.AddProcess(indexQueue)

		readsPaths1 := []string{}
		readsPaths2 := []string{}
		for _, readsIdx := range readsIndexes[sampleType] {
			readsPaths1 = append(readsPaths1, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R1.fastq.gz")
			readsPaths2 = append(readsPaths2, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R2.fastq.gz")
		}

		// Align samples
		readsFQ1 := sp.NewIPQueue(readsPaths1...)
		pr.AddProcess(readsFQ1)

		readsFQ2 := sp.NewIPQueue(readsPaths2...)
		pr.AddProcess(readsFQ2)

		alignSamples := NewBwaAlign(pr, "align_samples", sampleType, refFasta, refIndex)
		alignSamples.InReads1().Connect(readsFQ1.Out)
		alignSamples.InReads2().Connect(readsFQ2.Out)
		alignSamples.PPIndexNo().Connect(indexQueue.Out)

		// Merge BAMs
		streamToSubstream := spcomp.NewStreamToSubStream()
		streamToSubstream.In.Connect(alignSamples.OutBam())
		pr.AddProcess(streamToSubstream)

		mergeBams := NewSamtoolsMerge(pr, "merge_bams", sampleType, tmpDir)
		mergeBams.InBams().Connect(streamToSubstream.OutSubStream)

		// Mark Duplicates
		markDupes := NewGATKMarkDuplicates(pr, "mark_duplicates", sampleType, sampleIdx, appsDir, tmpDir)
		markDupes.InBam().Connect(mergeBams.OutMergedBam())
		markDupesProcs[sampleType] = markDupes
	}

	// Re-align Reads - Create Targets
	realignCreateTargets := NewGATKRealignCreateTargets(pr, "realign_create_targets", appsDir, tmpDir)
	realignCreateTargets.In("bamnormal").Connect(markDupesProcs["normal"].OutBam())
	realignCreateTargets.In("bamtumor").Connect(markDupesProcs["tumor"].OutBam())

	// Re-align Reads - Re-align Indels
	realignIndels := NewGATKRealignIndels(pr, "realign_indels", appsDir, refDir, tmpDir)
	realignIndels.InIntervals().Connect(realignCreateTargets.OutIntervals())
	realignIndels.InBamNormal().Connect(markDupesProcs["normal"].OutBam())
	realignIndels.InBamTumor().Connect(markDupesProcs["tumor"].OutBam())

	for _, sampleType := range sampleTypes {
		// Re-calibrate reads
		reCalibrate := NewGATKRecalibrate(pr, "recalibrate", sampleType, appsDir, refDir, tmpDir)
		reCalibrate.InRealBam().Connect(realignIndels.Out("realbam" + sampleType))

		// Print reads
		printReads := NewGATKPrintReads(pr, "print_reads", sampleType, appsDir, refDir)
		printReads.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
		printReads.In("recaltable").Connect(reCalibrate.Out("recaltable"))
		mainWfSink.Connect(printReads.Out("recalbam"))
	}

	// Run
	pr.AddProcess(mainWfSink)
	pr.Run()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Sub-workflows
////////////////////////////////////////////////////////////////////////////////////////////////////

// ----------------------------------------------------------------------------
// Data download workflow
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

////////////////////////////////////////////////////////////////////////////////////////////////////
// Component library
////////////////////////////////////////////////////////////////////////////////////////////////////

// ----------------------------------------------------------------------------
// BWA Align
// ----------------------------------------------------------------------------

type BwaAlign struct {
	*sp.SciProcess
}

func NewBwaAlign(pr *sp.PipelineRunner, procName string, sampleType string, refFasta string, refIndex string) *BwaAlign {
	inner := pr.NewFromShell(procName+"_"+sampleType, "bwa mem -R \"@RG\tID:"+sampleType+"_{p:indexno}\tSM:"+sampleType+"\tLB:"+sampleType+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
		"| samtools view -bS -t "+refIndex+" - "+
		"| samtools sort - > {o:bam}")
	inner.SetPathCustom("bam", func(t *sp.SciTask) string {
		outPath := tmpDir + "/" + sampleType + "_" + t.Params["indexno"] + ".bam"
		return outPath
	})
	return &BwaAlign{inner}
}

func (p *BwaAlign) PPIndexNo() *sp.ParamPort { return p.PP("indexno") }
func (p *BwaAlign) InReads1() *sp.FilePort   { return p.In("reads_1") }
func (p *BwaAlign) InReads2() *sp.FilePort   { return p.In("reads_2") }
func (p *BwaAlign) OutBam() *sp.FilePort     { return p.Out("bam") }

// ----------------------------------------------------------------------------
// Samtools Merge
// ----------------------------------------------------------------------------

type SamtoolsMerge struct {
	*sp.SciProcess
}

func NewSamtoolsMerge(pr *sp.PipelineRunner, procName string, sampleType string, tmpDir string) *SamtoolsMerge {
	inner := pr.NewFromShell(procName+"_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
	inner.SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")
	return &SamtoolsMerge{inner}
}

func (p *SamtoolsMerge) InBams() *sp.FilePort       { return p.In("bams") }
func (p *SamtoolsMerge) OutMergedBam() *sp.FilePort { return p.Out("mergedbam") }

// ----------------------------------------------------------------------------
// GATK Mark Duplicates
// ----------------------------------------------------------------------------

type GATKMarkDuplicates struct {
	*sp.SciProcess
}

func NewGATKMarkDuplicates(pr *sp.PipelineRunner, procName string, sampleType string, sampleIndex string, appsdir string, tmpDir string) *GATKMarkDuplicates {
	inner := pr.NewFromShell("mark_dupes_"+sampleType,
		`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+sampleIndex+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+sampleIndex+`.md{.bam.tmp,}.bai;`)
	inner.SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+sampleIndex+".md.bam")
	return &GATKMarkDuplicates{inner}
}

func (p *GATKMarkDuplicates) InBam() *sp.FilePort  { return p.In("bam") }
func (p *GATKMarkDuplicates) OutBam() *sp.FilePort { return p.Out("bam") }

// ----------------------------------------------------------------------------
// GATK Realign Create Targets
// ----------------------------------------------------------------------------

type GATKRealignCreateTargets struct {
	*sp.SciProcess
}

func NewGATKRealignCreateTargets(pr *sp.PipelineRunner, procName string, appsdir string, tmpDir string) *GATKRealignCreateTargets {
	inner := pr.NewFromShell(procName,
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
	inner.SetPathStatic("intervals", tmpDir+"/tiny.intervals")
	return &GATKRealignCreateTargets{inner}
}

func (p *GATKRealignCreateTargets) InBamNormal() *sp.FilePort  { return p.In("bamnormal") }
func (p *GATKRealignCreateTargets) InBamTumor() *sp.FilePort   { return p.In("bamtumor") }
func (p *GATKRealignCreateTargets) OutIntervals() *sp.FilePort { return p.Out("intervals") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKRealignIndels struct {
	*sp.SciProcess
}

func NewGATKRealignIndels(pr *sp.PipelineRunner, procName string, appsdir string, refDir string, tmpDir string) *GATKRealignIndels {
	inner := pr.NewFromShell(procName,
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
	inner.SetPathCustom("realbamnormal", func(t *sp.SciTask) string {
		path := t.InTargets["bamnormal"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	inner.SetPathCustom("realbamtumor", func(t *sp.SciTask) string {
		path := t.InTargets["bamtumor"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	return &GATKRealignIndels{inner}
}

func (p *GATKRealignIndels) InBamNormal() *sp.FilePort      { return p.In("bamnormal") }
func (p *GATKRealignIndels) InBamTumor() *sp.FilePort       { return p.In("bamtumor") }
func (p *GATKRealignIndels) InIntervals() *sp.FilePort      { return p.In("intervals") }
func (p *GATKRealignIndels) OutRealBamNormal() *sp.FilePort { return p.In("realbamnormal") }
func (p *GATKRealignIndels) OutRealBamTumor() *sp.FilePort  { return p.In("realbamtumor") }

// ----------------------------------------------------------------------------
// GATK Recalibrate
// ----------------------------------------------------------------------------

type GATKRecalibrate struct {
	*sp.SciProcess
}

func NewGATKRecalibrate(pr *sp.PipelineRunner, procName string, sampleType string, appsDir string, refDir string, tmpDir string) *GATKRecalibrate {
	inner := pr.NewFromShell(procName+"_"+sampleType,
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
	inner.SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")
	return &GATKRecalibrate{inner}
}

func (p *GATKRecalibrate) InRealBam() *sp.FilePort     { return p.In("realbam") }
func (p *GATKRecalibrate) OutRecalTable() *sp.FilePort { return p.Out("recaltable") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKPrintReads struct {
	*sp.SciProcess
}

func NewGATKPrintReads(pr *sp.PipelineRunner, procName string, sampleType string, appsdir string, refDir string) *GATKPrintReads {
	inner := pr.NewFromShell(procName+"_"+sampleType,
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
	inner.SetPathStatic("recalbam", sampleType+".recal.bam")
	return &GATKPrintReads{inner}
}

func (p *GATKPrintReads) InRealBam() *sp.FilePort   { return p.In("realbam") }
func (p *GATKPrintReads) OutRecalBam() *sp.FilePort { return p.Out("recalbam") }
