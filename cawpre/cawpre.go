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
	maxTasks   = flag.Int("maxtasks", 4, "Max number of local cores to use")
	procsRegex = flag.String("procs", "", "A regex specifying which processes (by name) to run up to")
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

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------
	flag.Parse()
	wf := sp.NewWorkflow("caw-preproc", *maxTasks)

	downloadApps := wf.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	downloadApps.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")

	unTgzApps := wf.NewProc("untgz_apps", "tar -zxvf {i:tgz} -C "+dataDir+" && echo untar_done > {o:done}")
	unTgzApps.SetPathStatic("done", dataDir+"/apps/done.flag")
	unTgzApps.In("tgz").Connect(downloadApps.Out("apps"))

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------
	refFasta := refDir + "/human_g1k_v37_decoy.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta.fai"

	indexes := map[string][]string{
		"normal": []string{"1", "2", "4", "7", "8"},
		"tumor":  []string{"1", "2", "3", "5", "6", "7"},
	}

	// Init some process "holders"
	markDuplicatesProcs := map[string]*sp.Process{}
	streamToSubstream := map[string]*spcomp.StreamToSubStream{}

	for i, sampleType := range []string{"normal", "tumor"} {
		sampleType := sampleType // Create local copy of variable. Needed to work around Go's funny behaviour of closures on loop variables
		si := strconv.Itoa(i)

		streamToSubstream[sampleType] = spcomp.NewStreamToSubStream(wf, "stream_to_substream_"+sampleType)
		for _, idx := range indexes[sampleType] {
			fastqPaths1 := origDataDir + "/tiny_" + sampleType + "_L00" + idx + "_R1.fastq.gz"
			fastqPaths2 := origDataDir + "/tiny_" + sampleType + "_L00" + idx + "_R2.fastq.gz"

			readsSourceFastQ1 := spcomp.NewFileSource(wf, "reads_fastq1_"+sampleType+"_idx"+idx, fastqPaths1)
			readsSourceFastQ2 := spcomp.NewFileSource(wf, "reads_fastq2_"+sampleType+"_idx"+idx, fastqPaths2)

			// --------------------------------------------------------------------------------
			// Align samples
			// --------------------------------------------------------------------------------
			alignSamples := wf.NewProc("align_samples_"+sampleType+"_idx"+idx, `bwa mem \
			-R "@RG\tID:`+sampleType+`_{p:index}\tSM:`+sampleType+`\tLB:`+sampleType+`\tPL:illumina" -B 3 -t 4 -M `+refFasta+` {i:reads1} {i:reads2} \
				| samtools view -bS -t `+refIndex+` - \
				| samtools sort - > {o:bam} # {i:untardone}`)
			alignSamples.In("reads1").Connect(readsSourceFastQ1.Out())
			alignSamples.In("reads2").Connect(readsSourceFastQ2.Out())
			alignSamples.In("untardone").Connect(unTgzApps.Out("done"))
			alignSamples.ParamInPort("index").ConnectStr(idx)
			alignSamples.SetPathCustom("bam", func(t *sp.Task) string {
				return tmpDir + "/" + sampleType + "_" + t.Param("index") + ".bam"
			})

			streamToSubstream[sampleType].In().Connect(alignSamples.Out("bam"))
		}

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------
		mergeBams := wf.NewProc("merge_bams_"+sampleType, "samtools merge -f {o:mergedbam} {i:bams:r: }")
		mergeBams.In("bams").Connect(streamToSubstream[sampleType].OutSubStream())
		mergeBams.SetPathStatic("mergedbam", tmpDir+"/"+sampleType+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------
		markDuplicates := wf.NewProc("mark_dupes_"+sampleType,
			`java -Xmx15g -jar `+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:bam} \
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+si+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+si+`.md{.bam.tmp,}.bai;`)
		markDuplicates.SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+si+".md.bam")
		markDuplicates.In("bam").Connect(mergeBams.Out("mergedbam"))
		// Save in map for later use
		markDuplicatesProcs[sampleType] = markDuplicates
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------
	realignCreateTargets := wf.NewProc("realign_create_targets",
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
	realignCreateTargets.SetPathStatic("intervals", tmpDir+"/tiny.intervals")
	realignCreateTargets.In("bamnormal").Connect(markDuplicatesProcs["normal"].Out("bam"))
	realignCreateTargets.In("bamtumor").Connect(markDuplicatesProcs["tumor"].Out("bam"))

	// --------------------------------------------------------------------------------
	// Re-align Reads - Re-align Indels
	// --------------------------------------------------------------------------------
	realignIndels := wf.NewProc("realign_indels",
		`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T IndelRealigner \
			-I {i:bamnormal} \
			-I {i:bamtumor} \
			-R `+refDir+`/human_g1k_v37_decoy.fasta \
			-targetIntervals {i:intervals} \
			-known `+refDir+`/1000G_phase1.indels.b37.vcf \
			-known `+refDir+`/Mills_and_1000G_gold_standard.indels.b37.vcf \
			-XL hs37d5 \
			-XL NC_007605 \
			-nWayOut '.real.bam' \
			&& mv *.md.real.ba* tmp/ # {o:realbamnormal} {o:realbamtumor}`) // Ugly hack to work around the lack of control induced by the -nWayOut way of specifying file name
	realignIndels.SetPathReplace("bamnormal", "realbamnormal", ".bam", ".real.bam")
	realignIndels.SetPathReplace("bamtumor", "realbamtumor", ".bam", ".real.bam")
	realignIndels.In("intervals").Connect(realignCreateTargets.Out("intervals"))
	realignIndels.In("bamnormal").Connect(markDuplicatesProcs["normal"].Out("bam"))
	realignIndels.In("bamtumor").Connect(markDuplicatesProcs["tumor"].Out("bam"))

	// --------------------------------------------------------------------------------
	// Re-calibrate reads
	// --------------------------------------------------------------------------------
	for _, sampleType := range []string{"normal", "tumor"} {
		// Re-calibrate
		reCalibrate := wf.NewProc("recalibrate_"+sampleType,
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
		reCalibrate.SetPathStatic("recaltable", tmpDir+"/"+sampleType+".recal.table")
		reCalibrate.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))

		// Print reads
		printReads := wf.NewProc("print_reads_"+sampleType,
			`java -Xmx3g -jar `+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
				-R `+refDir+`/human_g1k_v37_decoy.fasta \
				-nct 4 \
				-I {i:realbam} \
				-XL hs37d5 \
				-XL NC_007605 \
				--BQSR {i:recaltable} \
				-o {o:recalbam} \
				&& fname={o:recalbam} \
				&& mv $fname".bai" ${fname%.bam.tmp}.bai;`)
		printReads.SetPathStatic("recalbam", sampleType+".recal.bam")
		printReads.In("realbam").Connect(realignIndels.Out("realbam" + sampleType))
		printReads.In("recaltable").Connect(reCalibrate.Out("recaltable"))
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

	wf.RunToRegex(*procsRegex)
}
