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
	maxTasks   = flag.Int("maxtasks", 2, "Max number of local cores to use")
	procsRegex = flag.String("procs", "print_reads.*", "A regex specifying which processes (by name) to run up to")
)

func main() {
	// ------------------------------------------------
	// Set up paths
	// ------------------------------------------------
	tmpDir := "tmp"
	appsDir := "data/apps"
	refDir := appsDir + "/ref"
	origDataDir := appsDir + "/data"
	dataDir := "data"

	// ----------------------------------------------------------------------------
	// Data Download part of the workflow
	// ----------------------------------------------------------------------------
	flag.Parse()
	wf := sp.NewWorkflow("caw-preproc", *maxTasks)

	downloadApps := wf.NewProc("download_apps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	downloadApps.SetOut("apps", dataDir+"/apps.tar.gz")

	unTgzApps := wf.NewProc("untgz_apps", "tar -zxvf {i:tgz} -C ../"+dataDir+" && echo untar_done > {o:done}")
	unTgzApps.SetOut("done", dataDir+"/apps/done.flag")
	unTgzApps.In("tgz").From(downloadApps.Out("apps"))

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------
	refFasta := refDir + "/human_g1k_v37_decoy.chr1.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta.chr1.fai"

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
			alignSamples := wf.NewProc("align_samples_"+sampleType+"_idx"+idx, "../"+appsDir+`/bwa-0.7.15/bwa mem \
			-R "@RG\tID:`+sampleType+`_{p:index}\tSM:`+sampleType+`\tLB:`+sampleType+`\tPL:illumina" -B 3 -t 4 -M ../`+refFasta+` {i:reads1} {i:reads2} \
				| ../`+appsDir+`/samtools-1.3.1/samtools view -bS -t ../`+refIndex+` - \
				| ../`+appsDir+`/samtools-1.3.1/samtools sort - > {o:bam} # {i:untardone}`)
			alignSamples.In("reads1").From(readsSourceFastQ1.Out())
			alignSamples.In("reads2").From(readsSourceFastQ2.Out())
			alignSamples.In("untardone").From(unTgzApps.Out("done"))
			alignSamples.InParam("index").FromStr(idx)
			alignSamples.SetOut("bam", tmpDir+"/"+sampleType+"_{p:index}.bam")

			streamToSubstream[sampleType].In().From(alignSamples.Out("bam"))
		}

		// --------------------------------------------------------------------------------
		// Merge BAMs
		// --------------------------------------------------------------------------------
		mergeBams := wf.NewProc("merge_bams_"+sampleType, "../"+appsDir+"/samtools-1.3.1/samtools merge -f {o:mergedbam} {i:bams|join: }")
		mergeBams.In("bams").From(streamToSubstream[sampleType].OutSubStream())
		mergeBams.SetOut("mergedbam", tmpDir+"/"+sampleType+".bam")

		// --------------------------------------------------------------------------------
		// Mark Duplicates
		// --------------------------------------------------------------------------------
		markDuplicates := wf.NewProc("mark_dupes_"+sampleType,
			`java -Xmx15g -jar ../`+appsDir+`/picard-tools-1.118/MarkDuplicates.jar \
				INPUT={i:inbam} \
				METRICS_FILE=../`+tmpDir+`/`+sampleType+`_`+si+`.md.bam \
				TMP_DIR=../`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:outbam}`)
		markDuplicates.SetOut("outbam", tmpDir+"/"+sampleType+".md.bam")
		markDuplicates.In("inbam").From(mergeBams.Out("mergedbam"))
		// Save in map for later use
		markDuplicatesProcs[sampleType] = markDuplicates
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------
	realignCreateTargets := wf.NewProc("realign_create_targets",
		`java -Xmx3g -jar ../`+appsDir+`/gatk/GenomeAnalysisTK.jar -T RealignerTargetCreator  \
				-I {i:bamnormal} \
				-I {i:bamtumor} \
				-R ../`+refDir+`/human_g1k_v37_decoy.chr1.fasta \
				-known ../`+refDir+`/1000G_phase1.indels.b37.chr1.vcf.gz \
				-known ../`+refDir+`/Mills_and_1000G_gold_standard.indels.b37.chr1.vcf.gz \
				-nt 4 \
				-o {o:intervals}`)
	realignCreateTargets.SetOut("intervals", tmpDir+"/tiny.intervals")
	realignCreateTargets.In("bamnormal").From(markDuplicatesProcs["normal"].Out("outbam"))
	realignCreateTargets.In("bamtumor").From(markDuplicatesProcs["tumor"].Out("outbam"))

	// --------------------------------------------------------------------------------
	// Re-align Reads - Re-align Indels
	// --------------------------------------------------------------------------------
	realignIndels := wf.NewProc("realign_indels",
		`java -Xmx3g -jar ../`+appsDir+`/gatk/GenomeAnalysisTK.jar -T IndelRealigner \
			-I {i:bamnormal} \
			-I {i:bamtumor} \
			-R ../`+refDir+`/human_g1k_v37_decoy.chr1.fasta \
			-targetIntervals {i:intervals} \
			-known ../`+refDir+`/1000G_phase1.indels.b37.chr1.vcf.gz \
			-known ../`+refDir+`/Mills_and_1000G_gold_standard.indels.b37.chr1.vcf.gz \
			-nWayOut '.real.bam' \
			&& mv *.md.real.ba* tmp/ # {o:realbamnormal} {o:realbamtumor}`) // Ugly hack to work around the lack of control induced by the -nWayOut way of specifying file name
	realignIndels.SetOut("realbamnormal", "{i:bamnormal|%.bam}.real.bam")
	realignIndels.SetOut("realbamtumor", "{i:bamtumor|%.bam}.real.bam")
	realignIndels.In("intervals").From(realignCreateTargets.Out("intervals"))
	realignIndels.In("bamnormal").From(markDuplicatesProcs["normal"].Out("outbam"))
	realignIndels.In("bamtumor").From(markDuplicatesProcs["tumor"].Out("outbam"))

	// --------------------------------------------------------------------------------
	// Re-calibrate reads
	// --------------------------------------------------------------------------------
	for _, sampleType := range []string{"normal", "tumor"} {
		// Re-calibrate
		reCalibrate := wf.NewProc("recalibrate_"+sampleType,
			`java -Xmx3g -Djava.io.tmpdir=../`+tmpDir+` -jar ../`+appsDir+`/gatk/GenomeAnalysisTK.jar -T BaseRecalibrator \
				-R ../`+refDir+`/human_g1k_v37_decoy.chr1.fasta \
				-I {i:realbam} \
				-knownSites ../`+refDir+`/dbsnp_138.b37.chr1.vcf.gz \
				-knownSites ../`+refDir+`/1000G_phase1.indels.b37.chr1.vcf.gz \
				-knownSites ../`+refDir+`/Mills_and_1000G_gold_standard.indels.b37.chr1.vcf.gz \
				-nct 4 \
				-l INFO \
				-o {o:recaltable}`)
		reCalibrate.SetOut("recaltable", tmpDir+"/"+sampleType+".recal.table")
		reCalibrate.In("realbam").From(realignIndels.Out("realbam" + sampleType))

		// Print reads
		printReads := wf.NewProc("print_reads_"+sampleType,
			`java -Xmx3g -jar ../`+appsDir+`/gatk/GenomeAnalysisTK.jar -T PrintReads \
				-R ../`+refDir+`/human_g1k_v37_decoy.chr1.fasta \
				-nct 4 \
				-I {i:realbam} \
				--BQSR {i:recaltable} \
				-o {o:recalbam} \
				&& fname={o:recalbam}`)
		printReads.SetOut("recalbam", sampleType+".recal.bam")
		printReads.In("realbam").From(realignIndels.Out("realbam" + sampleType))
		printReads.In("recaltable").From(reCalibrate.Out("recaltable"))
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
