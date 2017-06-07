package main

import (
	. "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
	"strconv"
	"strings"
)

// Set up some constant stuff like paths
const (
	tmpDir      = "tmp"
	dataDir     = "dat"
	appsDir     = dataDir + "/apps"
	origDataDir = appsDir + "/pipeline_test/data"
	refDir      = appsDir + "/pipeline_test/ref"
	refFasta    = refDir + "/human_g1k_v37_decoy.fasta"
	refIndex    = refDir + "/human_g1k_v37_decoy.fasta.fai"
)

// The main "variable" parameter inputs to the workflow
var (
	sampleTypes  = []string{"normal", "tumor"}
	readsIndexes = map[string][]string{
		"normal": {"1", "2", "4", "7", "8"},
		"tumor":  {"1", "2", "3", "5", "6", "7"},
	}
)

func main() {

	// Run the Data Download part of the workflow
	downloadDataWorkflow := NewDownloadWorkflow(dataDir)
	downloadDataWorkflow.Run()

	// ------------------------------------------------
	// Main workflow starts here
	// ------------------------------------------------

	// Some technical initialization
	pr := NewPipelineRunner()
	mainWfSink := NewSink()

	// Init a process "holder" for the final process in this part, as we need
	// to access the normal and tumor verions specifically
	markDupesProcs := map[string]*GATKMarkDuplicates{}

	for sampleIdxStr, sampleType := range sampleTypes {
		sampleIdx := strconv.Itoa(sampleIdxStr)

		// Some parameter book-keeping
		indexGen := spcomp.NewStringGen(readsIndexes[sampleType]...)
		pr.AddProcess(indexGen)

		readsPaths1 := []string{}
		readsPaths2 := []string{}
		for _, readsIdx := range readsIndexes[sampleType] {
			readsPaths1 = append(readsPaths1, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R1.fastq.gz")
			readsPaths2 = append(readsPaths2, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R2.fastq.gz")
		}

		// Align samples
		readsGen1 := NewIPGen(readsPaths1...)
		pr.AddProcess(readsGen1)

		readsGen2 := NewIPGen(readsPaths2...)
		pr.AddProcess(readsGen2)

		alignSamples := NewBwaAlign(pr, "align_samples", sampleType, refFasta, refIndex)
		alignSamples.InReads1().Connect(readsGen1.Out)
		alignSamples.InReads2().Connect(readsGen2.Out)
		alignSamples.ParamIndexNo().Connect(indexGen.Out)

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
	*Pipeline
}

func NewDownloadWorkflow(dataDir string) *DownloadWorkflow {
	wf := &DownloadWorkflow{NewPipeline()}

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
	*SciProcess
}

func NewBwaAlign(pr *PipelineRunner, procName string, sampleType string, refFasta string, refIndex string) *BwaAlign {
	inner := pr.NewFromShell(procName+"_"+sampleType, "bwa mem -R \"@RG\tID:"+sampleType+"_{p:indexno}\tSM:"+sampleType+"\tLB:"+sampleType+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
		"| samtools view -bS -t "+refIndex+" - "+
		"| samtools sort - > {o:bam}")
	inner.SetPathCustom("bam", func(t *SciTask) string {
		outPath := tmpDir + "/" + sampleType + "_" + t.Params["indexno"] + ".bam"
		return outPath
	})
	return &BwaAlign{inner}
}

func (p *BwaAlign) ParamIndexNo() *ParamPort { return p.ParamPort("indexno") }
func (p *BwaAlign) InReads1() *FilePort      { return p.In("reads_1") }
func (p *BwaAlign) InReads2() *FilePort      { return p.In("reads_2") }
func (p *BwaAlign) OutBam() *FilePort        { return p.Out("bam") }

// ----------------------------------------------------------------------------
// Samtools Merge
// ----------------------------------------------------------------------------

type SamtoolsMerge struct {
	*SciProcess
}

func NewSamtoolsMerge(pr *PipelineRunner, procName string, sampleType string, tmpDir string) *SamtoolsMerge {
	inner := pr.NewFromShell(procName+"_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
	inner.SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")
	return &SamtoolsMerge{inner}
}

func (p *SamtoolsMerge) InBams() *FilePort       { return p.In("bams") }
func (p *SamtoolsMerge) OutMergedBam() *FilePort { return p.Out("mergedbam") }

// ----------------------------------------------------------------------------
// GATK Mark Duplicates
// ----------------------------------------------------------------------------

type GATKMarkDuplicates struct {
	*SciProcess
}

func NewGATKMarkDuplicates(pr *PipelineRunner, procName string, sampleType string, sampleIndex string, appsdir string, tmpDir string) *GATKMarkDuplicates {
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

func (p *GATKMarkDuplicates) InBam() *FilePort  { return p.In("bam") }
func (p *GATKMarkDuplicates) OutBam() *FilePort { return p.Out("bam") }

// ----------------------------------------------------------------------------
// GATK Realign Create Targets
// ----------------------------------------------------------------------------

type GATKRealignCreateTargets struct {
	*SciProcess
}

func NewGATKRealignCreateTargets(pr *PipelineRunner, procName string, appsdir string, tmpDir string) *GATKRealignCreateTargets {
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

func (p *GATKRealignCreateTargets) InBamNormal() *FilePort  { return p.In("bamnormal") }
func (p *GATKRealignCreateTargets) InBamTumor() *FilePort   { return p.In("bamtumor") }
func (p *GATKRealignCreateTargets) OutIntervals() *FilePort { return p.Out("intervals") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKRealignIndels struct {
	*SciProcess
}

func NewGATKRealignIndels(pr *PipelineRunner, procName string, appsdir string, refDir string, tmpDir string) *GATKRealignIndels {
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
	inner.SetPathCustom("realbamnormal", func(t *SciTask) string {
		path := t.InTargets["bamnormal"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	inner.SetPathCustom("realbamtumor", func(t *SciTask) string {
		path := t.InTargets["bamtumor"].GetPath()
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	return &GATKRealignIndels{inner}
}

func (p *GATKRealignIndels) InBamNormal() *FilePort      { return p.In("bamnormal") }
func (p *GATKRealignIndels) InBamTumor() *FilePort       { return p.In("bamtumor") }
func (p *GATKRealignIndels) InIntervals() *FilePort      { return p.In("intervals") }
func (p *GATKRealignIndels) OutRealBamNormal() *FilePort { return p.In("realbamnormal") }
func (p *GATKRealignIndels) OutRealBamTumor() *FilePort  { return p.In("realbamtumor") }

// ----------------------------------------------------------------------------
// GATK Recalibrate
// ----------------------------------------------------------------------------

type GATKRecalibrate struct {
	*SciProcess
}

func NewGATKRecalibrate(pr *PipelineRunner, procName string, sampleType string, appsDir string, refDir string, tmpDir string) *GATKRecalibrate {
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

func (p *GATKRecalibrate) InRealBam() *FilePort     { return p.In("realbam") }
func (p *GATKRecalibrate) OutRecalTable() *FilePort { return p.Out("recaltable") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKPrintReads struct {
	*SciProcess
}

func NewGATKPrintReads(pr *PipelineRunner, procName string, sampleType string, appsdir string, refDir string) *GATKPrintReads {
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

func (p *GATKPrintReads) InRealBam() *FilePort   { return p.In("realbam") }
func (p *GATKPrintReads) OutRecalBam() *FilePort { return p.Out("recalbam") }
