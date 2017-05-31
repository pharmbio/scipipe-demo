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
	dataDir := "dat"
	appsDir := dataDir + "/apps"
	refDir := appsDir + "/pipeline_test/ref"
	origDataDir := appsDir + "/pipeline_test/data"

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

	appsDirFanOut := spcomp.NewFanOut()
	appsDirFanOut.InFile.Connect(untarApps.GetOutPort("outdir"))
	wf.AddProcess(appsDirFanOut)

	appsDirMultiplicator := map[string]*FileMultiplicator{
		"normal": NewFileMultiplicator(5),
		"tumor":  NewFileMultiplicator(6),
	}
	wf.AddProcess(appsDirMultiplicator["normal"])
	wf.AddProcess(appsDirMultiplicator["tumor"])

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

	for i, st := range []string{"normal", "tumor"} {
		appsDirMultiplicator[st].In.Connect(appsDirFanOut.GetOutPort(st))

		si := strconv.Itoa(i)
		indexQueue[st] = NewParamQueue(indexes[st]...)
		wf.AddProcess(indexQueue[st])

		for _, idx := range indexes[st] {
			fqPaths1[st] = append(fqPaths1[st], origDataDir+"/tiny_"+st+"_L00"+idx+"_R1.fastq.gz")
			fqPaths2[st] = append(fqPaths2[st], origDataDir+"/tiny_"+st+"_L00"+idx+"_R2.fastq.gz")
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		readsFQ1[st] = sp.NewIPQueue(fqPaths1[st]...)
		wf.AddProcess(readsFQ1[st])
		readsFQ2[st] = sp.NewIPQueue(fqPaths2[st]...)
		wf.AddProcess(readsFQ2[st])

		alignSamples[st] = sp.NewFromShell("align_samples_"+st, "bwa mem -R \"@RG\tID:"+st+"_{p:index}\tSM:"+st+"\tLB:"+st+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads1} {i:reads2}"+
			"| samtools view -bS -t "+refIndex+" - "+
			"| samtools sort - > {o:bam} # {i:appsdir}")
		alignSamples[st].GetInPort("reads1").Connect(readsFQ1[st].Out)
		alignSamples[st].GetInPort("reads2").Connect(readsFQ2[st].Out)
		alignSamples[st].GetInPort("appsdir").Connect(appsDirMultiplicator[st].Out)
		alignSamples[st].ParamPorts["index"].Connect(indexQueue[st].Out)
		alignSamples[st].PathFormatters["bam"] = func(t *sp.SciTask) string {
			outPath := tmpDir + "/" + st + "_" + t.Params["index"] + ".bam"
			return outPath
		}
		wf.AddProcess(alignSamples[st])

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream[st] = spcomp.NewStreamToSubStream()
		streamToSubstream[st].In.Connect(alignSamples[st].GetOutPort("bam"))
		wf.AddProcess(streamToSubstream[st])

		mergeBams[st] = sp.NewFromShell("merge_bams_"+st, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams[st].GetInPort("bams").Connect(streamToSubstream[st].OutSubStream)
		mergeBams[st].SetPathStatic("mergedbam", tmpDir+"/"+st+".bam")
		wf.AddProcess(mergeBams[st])

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		markDupes[st] = sp.NewFromShell("mark_dupes_"+st,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+st+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+st+`_`+si+`.md{.bam.tmp,}.bai;`)
		markDupes[st].SetPathStatic("bam", tmpDir+"/"+st+"_"+si+".md.bam")
		markDupes[st].GetInPort("bam").Connect(mergeBams[st].GetOutPort("mergedbam"))
		wf.AddProcess(markDupes[st])
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

	for _, st := range []string{"normal", "tumor"} {

		realBamFanOut[st] = spcomp.NewFanOut()
		realBamFanOut[st].InFile.Connect(realignIndels.GetOutPort("realbam" + st))
		wf.AddProcess(realBamFanOut[st])

		reCalibrate[st] = sp.NewFromShell("recalibrate_"+st,
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
		reCalibrate[st].GetInPort("realbam").Connect(realBamFanOut[st].GetOutPort("recal"))
		reCalibrate[st].SetPathStatic("recaltable", tmpDir+"/"+st+".recal.table")
		wf.AddProcess(reCalibrate[st])

		printReads[st] = sp.NewFromShell("print_reads_"+st,
			`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-nct 4 \
				-I {i:realbam} \
				-XL hs37d5 \
				-XL NC_007605 \
				--BQSR {i:recaltable} \
				-o {o:recalbam};
				fname={o:recalbam};
				mv $fname ${fname%.bam.tmp.bai}.bai;`)
		printReads[st].GetInPort("realbam").Connect(realBamFanOut[st].GetOutPort("printreads"))
		printReads[st].GetInPort("recaltable").Connect(reCalibrate[st].GetOutPort("recaltable"))
		printReads[st].SetPathStatic("recalbam", st+".recal.bam")
		wf.AddProcess(printReads[st])

		mainWfSink.Connect(printReads[st].GetOutPort("recalbam"))
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
