package main

import (
	"strconv"
	"strings"

	. "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
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

	coresCnt := 4

	// Run the Data Download part of the workflow
	downloadDataWorkflow := NewDownloadWorkflow(dataDir, coresCnt)
	downloadDataWorkflow.Run()

	// ------------------------------------------------
	// Main workflow starts here
	// ------------------------------------------------
	wf := NewWorkflow("caw_wf", coresCnt)

	// Init a process "holder" for the final process in this part, as we need
	// to access the normal and tumor verions specifically
	markDupesProcs := map[string]*GATKMarkDuplicates{}

	for sampleIdxStr, sampleType := range sampleTypes {
		sampleIdx := strconv.Itoa(sampleIdxStr)

		// Some parameter book-keeping
		readsPaths1 := []string{}
		readsPaths2 := []string{}
		for _, readsIdx := range readsIndexes[sampleType] {
			readsPaths1 = append(readsPaths1, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R1.fastq.gz")
			readsPaths2 = append(readsPaths2, origDataDir+"/tiny_"+sampleType+"_L00"+readsIdx+"_R2.fastq.gz")
		}

		readsSrc1 := spcomp.NewFileSource(wf, "gen_readspaths1_"+sampleType, readsPaths1...)
		readsSrc2 := spcomp.NewFileSource(wf, "gen_readspaths2_"+sampleType, readsPaths2...)
		indexSrc := spcomp.NewParamSource(wf, "gen_readsidxes_"+sampleType, readsIndexes[sampleType]...)

		// Align samples
		alignSamples := NewBwaAlign(wf, "align_samples", sampleType, refFasta, refIndex)
		alignSamples.InReads1().Connect(readsSrc1.Out())
		alignSamples.InReads2().Connect(readsSrc2.Out())
		alignSamples.ParamIndexNo().Connect(indexSrc.Out())

		// Merge BAMs
		streamToSubstream := spcomp.NewStreamToSubStream(wf, "alignsamples_str2substr_"+sampleType)
		streamToSubstream.In().Connect(alignSamples.OutBam())

		mergeBams := NewSamtoolsMerge(wf, "merge_bams", sampleType, tmpDir)
		mergeBams.InBams().Connect(streamToSubstream.OutSubStream())

		// Mark Duplicates
		markDupes := NewGATKMarkDuplicates(wf, "mark_duplicates", sampleType, sampleIdx, appsDir, tmpDir)
		markDupes.InBam().Connect(mergeBams.OutMergedBam())
		markDupesProcs[sampleType] = markDupes
	}

	// Re-align Reads - Create Targets
	realignCreateTargets := NewGATKRealignCreateTargets(wf, "realign_create_targets", appsDir, tmpDir)
	realignCreateTargets.In("bamnormal").Connect(markDupesProcs["normal"].OutBam())
	realignCreateTargets.In("bamtumor").Connect(markDupesProcs["tumor"].OutBam())

	// Re-align Reads - Re-align Indels
	realignIndels := NewGATKRealignIndels(wf, "realign_indels", appsDir, refDir, tmpDir)
	realignIndels.InIntervals().Connect(realignCreateTargets.OutIntervals())
	realignIndels.InBamNormal().Connect(markDupesProcs["normal"].OutBam())
	realignIndels.InBamTumor().Connect(markDupesProcs["tumor"].OutBam())

	for _, sampleType := range sampleTypes {
		// Re-calibrate reads
		reCalibrate := NewGATKRecalibrate(wf, "recalibrate", sampleType, appsDir, refDir, tmpDir)
		reCalibrate.InRealBam().Connect(realignIndels.Out("realbam" + sampleType))

		// Print reads
		printReads := NewGATKPrintReads(wf, "print_reads", sampleType, appsDir, refDir)
		printReads.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
		printReads.In("recaltable").Connect(reCalibrate.Out("recaltable"))
	}

	// Run workflow
	wf.Run()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Sub-workflows
////////////////////////////////////////////////////////////////////////////////////////////////////

// ----------------------------------------------------------------------------
// Data download workflow
// ----------------------------------------------------------------------------

type DownloadWorkflow struct {
	*Workflow
}

func NewDownloadWorkflow(dataDir string, cores int) *DownloadWorkflow {
	wf := &DownloadWorkflow{NewWorkflow("dldata_wf", cores)}

	dwnld := wf.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	dwnld.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	unzip := wf.NewProc("unzip_apps", "zcat {i:targz} > {o:tar}")
	unzip.SetPathReplace("targz", "tar", ".gz", "")
	unzip.In("targz").Connect(dwnld.Out("apps"))

	untar := wf.NewProc("untar_apps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	untar.SetPathStatic("outdir", dataDir+"/apps")
	untar.In("tar").Connect(unzip.Out("tar"))

	return wf
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Component library
////////////////////////////////////////////////////////////////////////////////////////////////////

// ----------------------------------------------------------------------------
// BWA Align
// ----------------------------------------------------------------------------

type BwaAlign struct {
	*Process
}

func NewBwaAlign(wf *Workflow, procName string, sampleType string, refFasta string, refIndex string) *BwaAlign {
	innerProc := wf.NewProc(procName+"_"+sampleType, "bwa mem -R \"@RG\tID:"+sampleType+"_{p:indexno}\tSM:"+sampleType+"\tLB:"+sampleType+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
		"| samtools view -bS -t "+refIndex+" - "+
		"| samtools sort - > {o:bam}")
	innerProc.SetPathCustom("bam", func(t *Task) string {
		outPath := tmpDir + "/" + sampleType + "_" + t.Params["indexno"] + ".bam"
		return outPath
	})
	return &BwaAlign{innerProc}
}

func (p *BwaAlign) ParamIndexNo() *ParamInPort { return p.ParamInPort("indexno") }
func (p *BwaAlign) InReads1() *InPort          { return p.In("reads_1") }
func (p *BwaAlign) InReads2() *InPort          { return p.In("reads_2") }
func (p *BwaAlign) OutBam() *OutPort           { return p.Out("bam") }

// ----------------------------------------------------------------------------
// Samtools Merge
// ----------------------------------------------------------------------------

type SamtoolsMerge struct {
	*Process
}

func NewSamtoolsMerge(wf *Workflow, procName string, sampleType string, tmpDir string) *SamtoolsMerge {
	innerProc := wf.NewProc(procName+"_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
	innerProc.SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")
	return &SamtoolsMerge{innerProc}
}

func (p *SamtoolsMerge) InBams() *InPort        { return p.In("bams") }
func (p *SamtoolsMerge) OutMergedBam() *OutPort { return p.Out("mergedbam") }

// ----------------------------------------------------------------------------
// GATK Mark Duplicates
// ----------------------------------------------------------------------------

type GATKMarkDuplicates struct {
	*Process
}

func NewGATKMarkDuplicates(wf *Workflow, procName string, sampleType string, sampleIndex string, appsdir string, tmpDir string) *GATKMarkDuplicates {
	innerProc := wf.NewProc("mark_dupes_"+sampleType,
		`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+sampleIndex+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+sampleIndex+`.md{.bam.tmp,}.bai;`)
	innerProc.SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+sampleIndex+".md.bam")
	return &GATKMarkDuplicates{innerProc}
}

func (p *GATKMarkDuplicates) InBam() *InPort   { return p.In("bam") }
func (p *GATKMarkDuplicates) OutBam() *OutPort { return p.Out("bam") }

// ----------------------------------------------------------------------------
// GATK Realign Create Targets
// ----------------------------------------------------------------------------

type GATKRealignCreateTargets struct {
	*Process
}

func NewGATKRealignCreateTargets(wf *Workflow, procName string, appsdir string, tmpDir string) *GATKRealignCreateTargets {
	innerProc := wf.NewProc(procName,
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
	innerProc.SetPathStatic("intervals", tmpDir+"/tiny.intervals")
	return &GATKRealignCreateTargets{innerProc}
}

func (p *GATKRealignCreateTargets) InBamNormal() *InPort   { return p.In("bamnormal") }
func (p *GATKRealignCreateTargets) InBamTumor() *InPort    { return p.In("bamtumor") }
func (p *GATKRealignCreateTargets) OutIntervals() *OutPort { return p.Out("intervals") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKRealignIndels struct {
	*Process
}

func NewGATKRealignIndels(wf *Workflow, procName string, appsdir string, refDir string, tmpDir string) *GATKRealignIndels {
	innerProc := wf.NewProc(procName,
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
	innerProc.SetPathCustom("realbamnormal", func(t *Task) string {
		path := t.InPath("bamnormal")
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	innerProc.SetPathCustom("realbamtumor", func(t *Task) string {
		path := t.InPath("bamtumor")
		path = strings.Replace(path, ".bam", ".real.bam", -1)
		path = strings.Replace(path, tmpDir+"/", "", -1)
		return path
	})
	return &GATKRealignIndels{innerProc}
}

func (p *GATKRealignIndels) InBamNormal() *InPort       { return p.In("bamnormal") }
func (p *GATKRealignIndels) InBamTumor() *InPort        { return p.In("bamtumor") }
func (p *GATKRealignIndels) InIntervals() *InPort       { return p.In("intervals") }
func (p *GATKRealignIndels) OutRealBamNormal() *OutPort { return p.Out("realbamnormal") }
func (p *GATKRealignIndels) OutRealBamTumor() *OutPort  { return p.Out("realbamtumor") }

// ----------------------------------------------------------------------------
// GATK Recalibrate
// ----------------------------------------------------------------------------

type GATKRecalibrate struct {
	*Process
}

func NewGATKRecalibrate(wf *Workflow, procName string, sampleType string, appsDir string, refDir string, tmpDir string) *GATKRecalibrate {
	innerProc := wf.NewProc(procName+"_"+sampleType,
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
	innerProc.SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")
	return &GATKRecalibrate{innerProc}
}

func (p *GATKRecalibrate) InRealBam() *InPort      { return p.In("realbam") }
func (p *GATKRecalibrate) OutRecalTable() *OutPort { return p.Out("recaltable") }

// ----------------------------------------------------------------------------
// GATK Realign Indels
// ----------------------------------------------------------------------------

type GATKPrintReads struct {
	*Process
}

func NewGATKPrintReads(wf *Workflow, procName string, sampleType string, appsdir string, refDir string) *GATKPrintReads {
	innerProc := wf.NewProc(procName+"_"+sampleType,
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
	innerProc.SetPathStatic("recalbam", sampleType+".recal.bam")
	return &GATKPrintReads{innerProc}
}

func (p *GATKPrintReads) InRealBam() *InPort    { return p.In("realbam") }
func (p *GATKPrintReads) OutRecalBam() *OutPort { return p.Out("recalbam") }
