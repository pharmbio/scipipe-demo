package main

import (
	"flag"
	"os"
	"sort"
	"strconv"
	"strings"

	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
)

var (
	graph 	   = flag.Bool("graph", false, "Plot graph and nothing more")
	maxTasks   = flag.Int("maxtasks", 4, "Max number of local cores to use")
	procsRegex = flag.String("procs", "align.*", "A regex specifying which processes (by name) to run up to")
)

func main() {
	// ------------------------------------------------
	// Set up paths
	// ------------------------------------------------
	tmpDir := "tmp"
	appsDir := "data/apps"
	refDir := appsDir + "/pipeline_test/ref"
	origDataDir := appsDir + "/pipeline_test/data"
	dataDir := "data"
	smaxTasks := strconv.Itoa(*maxTasks)

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------
	flag.Parse()
	wf := sp.NewWorkflow("rnaseqpre", *maxTasks)

	downloadApps := wf.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	downloadApps.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	unTgzApps := wf.NewProc("untgz_apps", "tar -zxvf {i:tgz} -C "+dataDir+" && echo untar_done > {o:done}")
	unTgzApps.SetPathStatic("done", dataDir+"/apps/done.flag")
	unTgzApps.In("tgz").Connect(downloadApps.Out("apps"))

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------
	samplePrefixes := []string{"SRR3222409"}
	// refGTF := refDir + "/human_g1k_v37_decoy.fasta"
	starIndex := refDir + "/rnaseq/star"

	// Init some process "holders"
	streamToSubstream := map[string]*spcomp.StreamToSubStream{}
	starProcs := map[string]*sp.Process{}
	// samtoolsProcs := map[string]*sp.Process{}
	// qualimapProcs := map[string]*sp.Process{}
	// featurecountsProcs := map[string]*sp.Process{}
	// multiqcProcs := map[string]*sp.Process{}
	

	for i, samplePrefix := range samplePrefixes {
		samplePrefix := samplePrefix // Create local copy of variable. Needed to work around Go's funny behaviour of closures on loop variables
		// si := strconv.Itoa(i)

		i=i

		streamToSubstream[samplePrefix] = spcomp.NewStreamToSubStream(wf, "stream_to_substream_"+samplePrefix)
		for j := 1; j <= 2; j++ {

			sj := strconv.Itoa(j)

			// define input file
			fastqPath := origDataDir + "/" + samplePrefix + "_" + sj + ".chr11.fq.gz"
			readsSourceFastQ := spcomp.NewFileSource(wf, "fastqFile_fastqc_"+samplePrefix + "_" + sj + ".chr11.fq.gz", fastqPath)

			// --------------------------------------------------------------------------------
			// Quality reporting
			// --------------------------------------------------------------------------------
			fastQSamples := wf.NewProc("fastqc_sample_" + samplePrefix + "_" + sj,
							appsDir + "/FastQC-0.11.5/fastqc {i:reads} -o " + tmpDir + "/rnaseqpre/fastqc && echo fastqc_done > {o:done} # {i:untardone}")
			fastQSamples.In("reads").Connect(readsSourceFastQ.Out())
			fastQSamples.In("untardone").Connect(unTgzApps.Out("done"))
			fastQSamples.SetPathCustom("done", func(t *sp.Task) string {
				return tmpDir + "/rnaseqpre/fastqc/done.flag"  // .tmp not removed?
			})

			streamToSubstream[samplePrefix].In().Connect(fastQSamples.Out("done"))
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		// define input files
		fastqPath1 := origDataDir + "/" + samplePrefix + "_1.chr11.fq.gz"
		fastqPath2 := origDataDir + "/" + samplePrefix + "_2.chr11.fq.gz"
		readsSourceFastQ1 := spcomp.NewFileSource(wf, "fastqFile_align_"+samplePrefix + "_1.chr11.fq.gz", fastqPath1)
		readsSourceFastQ2 := spcomp.NewFileSource(wf, "fastqFile_align_"+samplePrefix + "_2.chr11.fq.gz", fastqPath2)

		alignSamples := wf.NewProc("align_samples_"+samplePrefix, appsDir+"/STAR-2.5.3a/STAR --genomeDir "+starIndex+" --readFilesIn {i:reads1} {i:reads2} --runThreadN "+smaxTasks+" --readFilesCommand zcat --outFileNamePrefix "+tmpDir+"/rnaseqpre/star/"+samplePrefix+".chr11. --outSAMtype BAM SortedByCoordinate # {i:fastqc} {o:bam.aligned}")

		alignSamples.In("reads1").Connect(readsSourceFastQ1.Out())
		alignSamples.In("reads2").Connect(readsSourceFastQ2.Out())
		alignSamples.In("fastqc").Connect(streamToSubstream[samplePrefix].OutSubStream())
		
		alignSamples.SetPathStatic("bam.aligned", tmpDir+"/rnaseqpre/star/"+samplePrefix+".chr11.bam")
		starProcs[samplePrefix] = alignSamples







	// 	// --------------------------------------------------------------------------------
	// 	// Mark Duplicates
	// 	// --------------------------------------------------------------------------------
	// 	markDuplicates := wf.NewProc("mark_dupes_"+sampleType,
	// 		`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
	// 			INPUT={i:bam} \
	// 			METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+si+`.md.bam \
	// 			TMP_DIR=`+tmpDir+` \
	// 			ASSUME_SORTED=true \
	// 			VALIDATION_STRINGENCY=LENIENT \
	// 			CREATE_INDEX=TRUE \
	// 			OUTPUT={o:bam}; \
	// 			mv `+tmpDir+`/`+sampleType+`_`+si+`.md{.bam.tmp,}.bai;`)
	// 	markDuplicates.SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+si+".md.bam")
	// 	markDuplicates.In("bam").Connect(mergeBams.Out("mergedbam"))
	// 	// Save in map for later use
	// 	markDuplicatesProcs[sampleType] = markDuplicates
	// }

	// // --------------------------------------------------------------------------------
	// // Re-align Reads - Create Targets
	// // --------------------------------------------------------------------------------
	// realignCreateTargets := wf.NewProc("realign_create_targets",
	// 	`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T RealignerTargetCreator  \
	// 			-I {i:bamnormal} \
	// 			-I {i:bamtumor} \
	// 			-R `+refDir+`/human_g1k_v37_decoy.fasta \
	// 			-known `+refDir+`/1000G_phase1.indels.b37.vcf \
	// 			-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
	// 			-nt 4 \
	// 			-XL hs37d5 \
	// 			-XL NC_007605 \
	// 			-o {o:intervals} && sleep 5`)
	// realignCreateTargets.SetPathStatic("intervals", tmpDir+"/tiny.intervals")
	// realignCreateTargets.In("bamnormal").Connect(markDuplicatesProcs["normal"].Out("bam"))
	// realignCreateTargets.In("bamtumor").Connect(markDuplicatesProcs["tumor"].Out("bam"))

	// // --------------------------------------------------------------------------------
	// // Re-align Reads - Re-align Indels
	// // --------------------------------------------------------------------------------
	// realignIndels := wf.NewProc("realign_indels",
	// 	`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T IndelRealigner \
	// 		-I {i:bamnormal} \
	// 		-I {i:bamtumor} \
	// 		-R `+refDir+`/human_g1k_v37_decoy.fasta \
	// 		-targetIntervals {i:intervals} \
	// 		-known `+refDir+`/1000G_phase1.indels.b37.vcf \
	// 		-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
	// 		-XL hs37d5 \
	// 		-XL NC_007605 \
	// 		-nWayOut '.real.bam' \
	// 		&& sleep 5 && for f in *md.real.bam; do mv "$f" "$f.tmp"; done && mv *.md.real.ba* tmp/ # {o:realbamnormal} {o:realbamtumor}`) // Ugly hack to work around the lack of control induced by the -nWayOut way of specifying file name
	// realignIndels.SetPathReplace("bamnormal", "realbamnormal", ".bam", ".real.bam")
	// realignIndels.SetPathReplace("bamtumor", "realbamtumor", ".bam", ".real.bam")
	// realignIndels.In("intervals").Connect(realignCreateTargets.Out("intervals"))
	// realignIndels.In("bamnormal").Connect(markDuplicatesProcs["normal"].Out("bam"))
	// realignIndels.In("bamtumor").Connect(markDuplicatesProcs["tumor"].Out("bam"))

	// // --------------------------------------------------------------------------------
	// // Re-calibrate reads
	// // --------------------------------------------------------------------------------
	// for _, sampleType := range []string{"normal", "tumor"} {
	// 	// Re-calibrate
	// 	reCalibrate := wf.NewProc("recalibrate_"+sampleType,
	// 		`java -Xmx3g -Djava.io.tmpdir=`+tmpDir+` -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T BaseRecalibrator \
	// 			-R `+refDir+`/human_g1k_v37_decoy.fasta \
	// 			-I {i:realbam} \
	// 			-knownSites `+refDir+`/dbsnp_138.b37.vcf \
	// 			-knownSites `+refDir+`/1000G_phase1.indels.b37.vcf \
	// 			-knownSites `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
	// 			-nct 4 \
	// 			-XL hs37d5 \
	// 			-XL NC_007605 \
	// 			-l INFO \
	// 			-o {o:recaltable} && sleep 5`)
	// 	reCalibrate.SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")
	// 	reCalibrate.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))

	// 	// Print reads
	// 	printReads := wf.NewProc("print_reads_"+sampleType,
	// 		`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
	// 			-R `+refDir+`/human_g1k_v37_decoy.fasta \
	// 			-nct 4 \
	// 			-I {i:realbam} \
	// 			-XL hs37d5 \
	// 			-XL NC_007605 \
	// 			--BQSR {i:recaltable} \
	// 			-o {o:recalbam} \
	// 			&& fname={o:recalbam} \
	// 			&& mv $fname".bai" ${fname%.bam.tmp}.bai && sleep 5`)
	// 	printReads.SetPathStatic("recalbam", sampleType+".recal.bam")
	// 	printReads.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
	// 	printReads.In("recaltable").Connect(reCalibrate.Out("recaltable"))
	}

	// Handle missing flags
	procNames := []string{}
	for procName := range wf.Procs() {
		procNames = append(procNames, procName)
	}
	sort.Strings(procNames)
	procNamesStr := strings.Join(procNames, "\n")
	if *procsRegex == "" {
		sp.Error.Println("You must specify a process name pattern. You can specify one of:" + procNamesStr)
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *graph {
		wf.PlotGraph("rnaseq.dot", true, true)
	} else {
		wf.RunToRegex(*procsRegex)
	}
}
