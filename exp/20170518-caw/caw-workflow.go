package main

import (
	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
	"strconv"
)

func main() {
	sp.InitLogInfo()

	// ------------------------------------------------
	// Set up paths
	// ------------------------------------------------

	tmpDir := "tmp"
	appsDir := "data/apps"
	refDir := appsDir + "/pipeline_test/ref"
	origDataDir := appsDir + "/pipeline_test/data"
	dataDir := "data"

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------

	wf := sp.NewPipelineRunner()

	dlApps := sp.NewFromShell("dlApps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	dlApps.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")
	wf.AddProcess(dlApps)

	unzipApps := sp.NewFromShell("unzipApps", "zcat {i:targz} > {o:tar}")
	unzipApps.SetPathReplace("targz", "tar", ".gz", "")
	unzipApps.GetInPort("targz").Connect(dlApps.GetOutPort("apps"))
	wf.AddProcess(unzipApps)

	untarApps := sp.NewFromShell("untarApps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	untarApps.SetPathStatic("outdir", dataDir+"/apps")
	untarApps.GetInPort("tar").Connect(unzipApps.GetOutPort("tar"))
	wf.AddProcess(untarApps)

	appsDirMultiplicator := NewFileMultiplicator(11)
	appsDirMultiplicator.In.Connect(untarApps.GetOutPort("outdir"))
	wf.AddProcess(appsDirMultiplicator)

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------

	refFasta := refDir + "/human_g1k_v37_decoy.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta.fai"

	fqPaths1 := map[string][]string{}
	fqPaths2 := map[string][]string{}

	indexes := map[string][]string{}
	indexes["normal"] = []string{"1", "2", "4", "7", "8"}
	indexes["tumor"] = []string{"1", "2", "3", "5", "6", "7"}
	indexQueue := map[string]*ParamQueue{}

	// Init some process "holders"
	alignSamples := map[string]*sp.SciProcess{}
	mergeBams := map[string]*sp.SciProcess{}
	markDupes := map[string]*sp.SciProcess{}

	readsFQ1 := map[string]*sp.IPQueue{}
	readsFQ2 := map[string]*sp.IPQueue{}
	streamToSubstream := map[string]*spcomp.StreamToSubStream{}

	// Init the main sink
	mainWfSink := sp.NewSink()

	for i, sampleType := range []string{"normal", "tumor"} {
		si := strconv.Itoa(i)
		indexQueue[sampleType] = NewParamQueue(indexes[sampleType]...)
		wf.AddProcess(indexQueue[sampleType])

		for _, idx := range indexes[sampleType] {
			fqPaths1[sampleType] = append(fqPaths1[sampleType], origDataDir+"/tiny_"+sampleType+"_L00"+idx+"_R1.fastq.gz")
			fqPaths2[sampleType] = append(fqPaths2[sampleType], origDataDir+"/tiny_"+sampleType+"_L00"+idx+"_R2.fastq.gz")
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		readsFQ1[sampleType] = sp.NewIPQueue(fqPaths1[sampleType]...)
		wf.AddProcess(readsFQ1[sampleType])
		readsFQ2[sampleType] = sp.NewIPQueue(fqPaths2[sampleType]...)
		wf.AddProcess(readsFQ2[sampleType])

		alignSamples[sampleType] = sp.NewFromShell("align_samples_"+sampleType, "bwa mem -R \"@RG\tID:"+sampleType+"_{p:index}\tSM:"+sampleType+"\tLB:"+sampleType+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads1} {i:reads2}"+
			"| samtools view -bS -t "+refIndex+" - "+
			"| samtools sort - > {o:bam} # {i:appsdir}")
		alignSamples[sampleType].GetInPort("reads1").Connect(readsFQ1[sampleType].Out)
		alignSamples[sampleType].GetInPort("reads2").Connect(readsFQ2[sampleType].Out)
		alignSamples[sampleType].GetInPort("appsdir").Connect(appsDirMultiplicator.Out)
		alignSamples[sampleType].ParamPorts["index"].Connect(indexQueue[sampleType].Out)
		alignSamples[sampleType].PathFormatters["bam"] = func(t *sp.SciTask) string {
			outPath := tmpDir + "/" + sampleType + "_" + t.Params["index"] + ".bam"
			return outPath
		}
		wf.AddProcess(alignSamples[sampleType])

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream[sampleType] = spcomp.NewStreamToSubStream()
		streamToSubstream[sampleType].In.Connect(alignSamples[sampleType].GetOutPort("bam"))
		wf.AddProcess(streamToSubstream[sampleType])

		mergeBams[sampleType] = sp.NewFromShell("merge_bams_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams[sampleType].GetInPort("bams").Connect(streamToSubstream[sampleType].OutSubStream)
		mergeBams[sampleType].SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")
		wf.AddProcess(mergeBams[sampleType])

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		markDupes[sampleType] = sp.NewFromShell("mark_dupes_"+sampleType,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+si+`.md{.bam.tmp,}.bai;`)
		markDupes[sampleType].SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+si+".md.bam")
		markDupes[sampleType].GetInPort("bam").Connect(mergeBams[sampleType].GetOutPort("mergedbam"))
		wf.AddProcess(markDupes[sampleType])
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------

	markDupesNormalFanOut := spcomp.NewFanOut()
	markDupesNormalFanOut.InFile.Connect(markDupes["normal"].GetOutPort("bam"))
	wf.AddProcess(markDupesNormalFanOut)

	markDupesTumorFanOut := spcomp.NewFanOut()
	markDupesTumorFanOut.InFile.Connect(markDupes["tumor"].GetOutPort("bam"))
	wf.AddProcess(markDupesTumorFanOut)

	realignCreateTargets := sp.NewFromShell("realign_create_targets",
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
	realignCreateTargets.GetInPort("bamnormal").Connect(markDupesNormalFanOut.GetOutPort("create_targets"))
	realignCreateTargets.GetInPort("bamtumor").Connect(markDupesTumorFanOut.GetOutPort("create_targets"))
	realignCreateTargets.SetPathStatic("intervals", tmpDir+"/tiny.intervals")
	wf.AddProcess(realignCreateTargets)

	// --------------------------------------------------------------------------------
	// Re-align Reads - Re-align Indels
	// --------------------------------------------------------------------------------

	realignIndels := sp.NewFromShell("realign_indels",
		`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T IndelRealigner \
			-I {i:bamnormal} \
			-I {i:bamtumor} \
			-R `+refDir+`/human_g1k_v37_decoy.fasta \
			-targetIntervals {i:intervals} \
			-known `+refDir+`/1000G_phase1.indels.b37.vcf \
			-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
			-XL hs37d5 \
			-XL NC_007605 \
			-nWayOut '.real.bam' # {o:realbamnormal} {o:realbamtumor}`)
	realignIndels.GetInPort("intervals").Connect(realignCreateTargets.GetOutPort("intervals"))
	realignIndels.GetInPort("bamnormal").Connect(markDupesNormalFanOut.GetOutPort("realign_indels"))
	realignIndels.GetInPort("bamtumor").Connect(markDupesTumorFanOut.GetOutPort("realign_indels"))
	realignIndels.SetPathReplace("bamnormal", "realbamnormal", ".bam", ".real.bam")
	realignIndels.SetPathReplace("bamtumor", "realbamtumor", ".bam", ".real.bam")
	wf.AddProcess(realignIndels)

	// --------------------------------------------------------------------------------
	// Re-calibrate reads
	// --------------------------------------------------------------------------------

	realBamFanOut := map[string]*spcomp.FanOut{}

	reCalibrate := map[string]*sp.SciProcess{}
	printReads := map[string]*sp.SciProcess{}

	for _, sampleType := range []string{"normal", "tumor"} {

		realBamFanOut[sampleType] = spcomp.NewFanOut()
		realBamFanOut[sampleType].InFile.Connect(realignIndels.GetOutPort("realbam" + sampleType))
		wf.AddProcess(realBamFanOut[sampleType])

		reCalibrate[sampleType] = sp.NewFromShell("recalibrate_"+sampleType,
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
		reCalibrate[sampleType].GetInPort("realbam").Connect(realBamFanOut[sampleType].GetOutPort("recal"))
		reCalibrate[sampleType].SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")
		wf.AddProcess(reCalibrate[sampleType])

		printReads[sampleType] = sp.NewFromShell("print_reads_"+sampleType,
			`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-nct 4 \
				-I {i:realbam} \
				-XL hs37d5 \
				-XL NC_007605 \
				--BQSR {i:recaltable} \
				-o {o:recalbam}`)
		printReads[sampleType].GetInPort("realbam").Connect(realBamFanOut[sampleType].GetOutPort("printreads"))
		printReads[sampleType].GetInPort("recaltable").Connect(reCalibrate[sampleType].GetOutPort("recaltable"))
		printReads[sampleType].SetPathStatic("recalbam", sampleType+".recal.bam")
		wf.AddProcess(printReads[sampleType])

		mainWfSink.Connect(printReads[sampleType].GetOutPort("recalbam"))
	}

	wf.AddProcess(mainWfSink)
	wf.Run()
}

// ----------------------------------------------------------------------------
// Helper processes
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
