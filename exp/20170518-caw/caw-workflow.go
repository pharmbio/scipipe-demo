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

func main() {
	// Output slightly more info than default
	sp.InitLogInfo()

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------

	pr := sp.NewPipelineRunner()

	downloadApps := pr.NewFromShell("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	downloadApps.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	unzipApps := pr.NewFromShell("unzip_apps", "zcat {i:targz} > {o:tar}")
	unzipApps.SetPathReplace("targz", "tar", ".gz", "")
	unzipApps.In("targz").Connect(downloadApps.Out("apps"))

	untarApps := pr.NewFromShell("untar_apps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	untarApps.SetPathStatic("outdir", dataDir+"/apps")
	untarApps.In("tar").Connect(unzipApps.Out("tar"))

	appsDirFanOut := spcomp.NewFanOut()
	appsDirFanOut.InFile.Connect(untarApps.Out("outdir"))
	pr.AddProcess(appsDirFanOut)

	appsDirMultiplicator := map[string]*FileMultiplicator{
		"normal": NewFileMultiplicator(5),
		"tumor":  NewFileMultiplicator(6),
	}
	pr.AddProcess(appsDirMultiplicator["normal"])
	pr.AddProcess(appsDirMultiplicator["tumor"])

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------

	refFasta := refDir + "/human_g1k_v37_decoy.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta.fai"

	fqPaths1 := map[string][]string{}
	fqPaths2 := map[string][]string{}

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
		appsDirMultiplicator[st].In.Connect(appsDirFanOut.Out(st))

		si := strconv.Itoa(i)

		indexQueue[st] = NewParamQueue(indexes[st]...)
		pr.AddProcess(indexQueue[st])

		stQueue[st] = NewParamQueue(sampleTypes[st]...)
		pr.AddProcess(stQueue[st])

		for _, idx := range indexes[st] {
			fqPaths1[st] = append(fqPaths1[st], origDataDir+"/tiny_"+st+"_L00"+idx+"_R1.fastq.gz")
			fqPaths2[st] = append(fqPaths2[st], origDataDir+"/tiny_"+st+"_L00"+idx+"_R2.fastq.gz")
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		readsFQ1[st] = sp.NewIPQueue(fqPaths1[st]...)
		pr.AddProcess(readsFQ1[st])
		readsFQ2[st] = sp.NewIPQueue(fqPaths2[st]...)
		pr.AddProcess(readsFQ2[st])

		alignSamples[st] = pr.NewFromShell("align_samples_"+st, "bwa mem -R \"@RG\tID:{p:smpltyp}_{p:indexno}\tSM:{p:smpltyp}\tLB:{p:smpltyp}\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads_1} {i:reads_2}"+
			"| samtools view -bS -t "+refIndex+" - "+
			"| samtools sort - > {o:bam} # {i:appsdir}")
		alignSamples[st].In("reads_1").Connect(readsFQ1[st].Out)
		alignSamples[st].In("reads_2").Connect(readsFQ2[st].Out)
		alignSamples[st].In("appsdir").Connect(appsDirMultiplicator[st].Out)
		alignSamples[st].PP("indexno").Connect(indexQueue[st].Out)
		alignSamples[st].PP("smpltyp").Connect(stQueue[st].Out)
		alignSamples[st].SetPathCustom("bam", func(t *sp.SciTask) string {
			outPath := tmpDir + "/" + t.Params["smpltyp"] + "_" + t.Params["indexno"] + ".bam"
			return outPath
		})

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------

		streamToSubstream[st] = spcomp.NewStreamToSubStream()
		streamToSubstream[st].In.Connect(alignSamples[st].Out("bam"))
		pr.AddProcess(streamToSubstream[st])

		mergeBams[st] = pr.NewFromShell("merge_bams_"+st, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams[st].In("bams").Connect(streamToSubstream[st].OutSubStream)
		mergeBams[st].SetPathStatic("mergedbam", tmpDir+"/"+st+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------

		markDupes[st] = pr.NewFromShell("mark_dupes_"+st,
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
		markDupes[st].In("bam").Connect(mergeBams[st].Out("mergedbam"))
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

	for _, st := range []string{"normal", "tumor"} {

		realBamFanOut[st] = spcomp.NewFanOut()
		realBamFanOut[st].InFile.Connect(realignIndels.Out("realbam" + st))
		pr.AddProcess(realBamFanOut[st])

		reCalibrate[st] = pr.NewFromShell("recalibrate_"+st,
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
		reCalibrate[st].In("realbam").Connect(realBamFanOut[st].Out("recal"))
		reCalibrate[st].SetPathStatic("recaltable", tmpDir+"/"+st+".recal.table")

		printReads[st] = pr.NewFromShell("print_reads_"+st,
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
		printReads[st].In("realbam").Connect(realBamFanOut[st].Out("printreads"))
		printReads[st].In("recaltable").Connect(reCalibrate[st].Out("recaltable"))
		printReads[st].SetPathStatic("recalbam", st+".recal.bam")

		mainWfSink.Connect(printReads[st].Out("recalbam"))
	}

	pr.AddProcess(mainWfSink)
	pr.Run()
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
