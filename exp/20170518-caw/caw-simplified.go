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

	pl := sp.NewPipeline()

	pl.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	pl.GetProc("download_apps").SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	pl.NewProc("unzip_apps", "zcat {i:targz} > {o:tar}")
	pl.GetProc("unzip_apps").SetPathReplace("targz", "tar", ".gz", "")
	pl.Connect("unzip_apps.targz <- download_apps.apps")

	pl.NewProc("untar_apps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	pl.GetProc("untar_apps").SetPathStatic("outdir", dataDir+"/apps")
	pl.Connect("untar_apps.tar <- unzip_apps.tar")

	// -> We are here <-

	appsDirMultipl := NewFileMultiplicator(11)
	appsDirMultipl.In.Connect(untarApps.GetOutPort("outdir"))

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

		pl.NewProc("align_samples_"+st,
			"bwa mem -R \"@RG\tID:"+st+"_{p:index}\tSM:"+st+"\tLB:"+st+"\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads1} {i:reads2}"+
				"| samtools view -bS -t "+refIndex+" - "+
				"| samtools sort - > {o:bam} # {i:appsdir}")
		pl.GetProc("align_samples_" + st).GetInPort("reads1").Connect(readsFQ1[st].Out)
		pl.GetProc("align_samples_" + st).GetInPort("reads2").Connect(readsFQ2[st].Out)
		pl.GetProc("align_samples_" + st).GetInPort("appsdir").Connect(appsDirMultipl.Out)
		pl.GetProc("align_samples_" + st).ParamPorts["index"].Connect(indexQueue[st].Out)
		pl.GetProc("align_samples_"+st).SetPathCustom("bam", func(t *sp.SciTask) string {
			st := st // needed to work around Go's funny behaviour of closures
			outPath := tmpDir + "/" + st + "_" + t.Params["index"] + ".bam"
			return outPath
		})

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream[st] = spcomp.NewStreamToSubStream()
		streamToSubstream[st].In.Connect(alignSamples[st].GetOutPort("bam"))
		wf.AddProcess(streamToSubstream[st])

		pl.NewProc("merge_bams_"+st, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		pl.GetProc("merge_bams_" + st).GetInPort("bams").Connect(streamToSubstream[st].OutSubStream)
		pl.GetProc("merge_bams_"+st).SetPathStatic("mergedbam", tmpDir+"/"+st+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		pl.NewProc("mark_dupes_"+st,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+st+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+st+`_`+si+`.md{.bam.tmp,}.bai;`)
		pl.GetProc("mark_dupes_"+st).SetPathStatic("bam", tmpDir+"/"+st+"_"+si+".md.bam")
		pl.GetProc("mark_dupes_" + st).GetInPort("bam").Connect(mergeBams[st].GetOutPort("mergedbam"))
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

	pl.NewProc("realign_create_targets",
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
	pl.GetProc("realign_create_targets").GetInPort("bamnormal").Connect(markDupesNormalFanOut.GetOutPort("create_targets"))
	pl.GetProc("realign_create_targets").GetInPort("bamtumor").Connect(markDupesTumorFanOut.GetOutPort("create_targets"))
	pl.GetProc("realign_create_targets").SetPathStatic("intervals", tmpDir+"/tiny.intervals")

	// --------------------------------------------------------------------------------
	// Re-align Reads - Re-align Indels
	// --------------------------------------------------------------------------------

	pl.NewProc("realign_indels",
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
	pl.GetProc("realign_indels").GetInPort("intervals").Connect(realignCreateTargets.GetOutPort("intervals"))
	pl.GetProc("realign_indels").GetInPort("bamnormal").Connect(markDupesNormalFanOut.GetOutPort("realign_indels"))
	pl.GetProc("realign_indels").GetInPort("bamtumor").Connect(markDupesTumorFanOut.GetOutPort("realign_indels"))
	pl.GetProc("realign_indels").SetPathReplace("bamnormal", "realbamnormal", ".bam", ".real.bam")
	pl.GetProc("realign_indels").SetPathReplace("bamtumor", "realbamtumor", ".bam", ".real.bam")

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

		pl.NewProc("recalibrate_"+st,
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
		pl.GetProc("recalibrate_" + st).GetInPort("realbam").Connect(realBamFanOut[st].GetOutPort("recal"))
		pl.GetProc("recalibrate_"+st).SetPathStatic("recaltable", tmpDir+"/"+st+".recal.table")

		pl.NewProc("print_reads_"+st,
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
		pl.GetProc("print_reads_" + st).GetInPort("realbam").Connect(realBamFanOut[st].GetOutPort("printreads"))
		pl.GetProc("print_reads_" + st).GetInPort("recaltable").Connect(reCalibrate[st].GetOutPort("recaltable"))
		pl.GetProc("print_reads_"+st).SetPathStatic("recalbam", st+".recal.bam")
	}

	pl.Run()
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
