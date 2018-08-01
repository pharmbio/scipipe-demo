package main

import (
	"flag"
	"fmt"
	"sort"
	"strconv"

	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
)

var (
	plot     = flag.Bool("plot", false, "Plot graph and nothing more")
	maxTasks = flag.Int("maxtasks", 4, "Max number of local cores to use")
	//procsRegex = flag.String("procs", "align.*", "A regex specifying which processes (by name) to run up to")
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
	wf := sp.NewWorkflow("rnaseqpre", *maxTasks)

	downloadApps := wf.NewProc("download_apps", "wget https://zenodo.org/record/1324426/files/scipipe-demo-apps.tar.gz?download=1 -O {o:apps}")
	downloadApps.SetOut("apps", dataDir+"/apps.tar.gz")

	unTgzApps := wf.NewProc("untgz_apps", "tar -zxvf {i:tgz} -C "+dataDir+" && echo untar_done > {o:done}")
	unTgzApps.SetOut("done", dataDir+"/apps/done.flag")
	unTgzApps.In("tgz").From(downloadApps.Out("apps"))

	// ----------------------------------------------------------------------------
	// Main Workflow
	// ----------------------------------------------------------------------------
	samplePrefixes := []string{"SRR3222409"}
	// refGTF := refDir + "/human_g1k_v37_decoy.fasta"
	starIndex := refDir + "/star"

	// Init process "holders"
	strToSubstrs := map[string]*spcomp.StreamToSubStream{}
	featureCountsProcs := []*sp.Process{}

	for _, samplePrefix := range samplePrefixes {
		samplePrefix := samplePrefix // Create local copy of variable. Needed to work around Go's funny behaviour of closures on loop variables
		// si := strconv.Itoa(i)

		strToSubstrs[samplePrefix] = spcomp.NewStreamToSubStream(wf, "collect_substream_"+samplePrefix)
		for j := 1; j <= 2; j++ {

			sj := strconv.Itoa(j)

			// define input file
			readsSourceFastQPath := origDataDir + "/" + samplePrefix + "_" + sj + ".chr11.fq.gz"
			readsSourceFastQ := spcomp.NewFileSource(wf, "fastqFile_fastqc_"+samplePrefix+"_"+sj, readsSourceFastQPath)

			// --------------------------------------------------------------------------------
			// Quality reporting
			// --------------------------------------------------------------------------------
			fastQSamples := wf.NewProc("fastqc_sample_"+samplePrefix+"_"+sj,
				"../"+appsDir+"/FastQC-0.11.5/fastqc {i:reads} -o "+tmpDir+"/rnaseqpre/fastqc && echo fastqc_done > {o:done} # {i:untardone}")
			fastQSamples.In("reads").From(readsSourceFastQ.Out())
			fastQSamples.In("untardone").From(unTgzApps.Out("done"))
			fastQSamples.SetOutFunc("done", func(t *sp.Task) string {
				return tmpDir + "/rnaseqpre/fastqc/done.flag"
			})

			strToSubstrs[samplePrefix].In().From(fastQSamples.Out("done"))
		}

		// --------------------------------------------------------------------------------
		// Align samples
		// --------------------------------------------------------------------------------
		// define input files
		fastqPath1 := origDataDir + "/" + samplePrefix + "_1.chr11.fq.gz"
		fastqPath2 := origDataDir + "/" + samplePrefix + "_2.chr11.fq.gz"
		readsSourceFastQ1 := spcomp.NewFileSource(wf, "fastqFile_align_"+samplePrefix+"_1.chr11.fq.gz", fastqPath1)
		readsSourceFastQ2 := spcomp.NewFileSource(wf, "fastqFile_align_"+samplePrefix+"_2.chr11.fq.gz", fastqPath2)

		alignSamples := wf.NewProc("align_samples_"+samplePrefix,
			"../"+appsDir+"/STAR-2.5.3a/STAR \\\n"+
				" --genomeDir ../"+starIndex+
				" --readFilesIn {i:reads1} {i:reads2} \\\n"+
				fs(" --runThreadN %d \\\n", *maxTasks)+
				" --readFilesCommand zcat \\\n"+
				" --outFileNamePrefix $(s={o:bam_aligned}; echo ${s%Aligned.sortedByCoord.out.bam}) \\\n"+
				" --outSAMtype BAM SortedByCoordinate && echo done > {o:bam_aligned}.done # {i:fastqc|join: } ")
		alignSamples.In("reads1").From(readsSourceFastQ1.Out())
		alignSamples.In("reads2").From(readsSourceFastQ2.Out())
		alignSamples.In("fastqc").From(strToSubstrs[samplePrefix].OutSubStream())
		alignSamples.SetOut("bam_aligned", tmpDir+"/rnaseqpre/star/"+samplePrefix+".chr11.Aligned.sortedByCoord.out.bam")

		createIndex := wf.NewProc("create_index_"+samplePrefix, `../`+appsDir+`/samtools-1.3.1/samtools index {i:bam_aligned}`)
		createIndex.SetOut("index", "{i:bam_aligned}.bai")
		createIndex.In("bam_aligned").From(alignSamples.Out("bam_aligned"))

		// QualiMap
		// mkdir -p "$tmpDir/rnaseqpre/qualimap/"
		// $appsDir/QualiMap-2.2/qualimap rnaseq -pe -bam $tmpDir/rnaseqpre/star/SRR3222409.chr11.Aligned.sortedByCoord.out.bam -gtf $refDir/Mus_musculus.GRCm38.92.chr11.gtf --outdir $tmpDir/rnaseqpre/qualimap/ --java-mem-size=4G > /dev/null 2>&1
		qcAlignment := wf.NewProc("qc_alignment_"+samplePrefix, `../`+appsDir+`/QualiMap-2.2/qualimap rnaseq -pe \
			-bam {i:bam_aligned} \
			-gtf ../`+refDir+`/Mus_musculus.GRCm38.92.chr11.gtf \
			--outdir `+tmpDir+`/rnaseqpre/qualimap/ \
			--java-mem-size=4G &> {o:stdout}`)
		qcAlignment.In("bam_aligned").From(alignSamples.Out("bam_aligned"))
		qcAlignment.SetOut("stdout", "{i:bam_aligned|%.bam}.qualimap.stdout.log")

		// Count features
		countFeatures := wf.NewProc("count_features_"+samplePrefix, `../`+appsDir+`/subread-1.5.2/bin/featureCounts -p \
			-a ../`+refDir+`/Mus_musculus.GRCm38.92.chr11.gtf -t gene -g gene_id -s 0 \
			-o {o:feature_counts} \
			{i:bam_aligned} # Extra dependency: {i:index}`)
		countFeatures.In("bam_aligned").From(alignSamples.Out("bam_aligned"))
		countFeatures.In("index").From(createIndex.Out("index"))
		countFeatures.SetOut("feature_counts", tmpDir+`/rnaseqpre/featurecounts/tableCounts`)

		featureCountsProcs = append(featureCountsProcs, countFeatures)
	}

	// Join feature count outputs into a substream to be joined in multiQC
	featureCountS2SS := spcomp.NewStreamToSubStream(wf, "featcnt_s2ss")
	for _, p := range featureCountsProcs {
		featureCountS2SS.In().From(p.Out("feature_counts"))
	}

	// MultiQC
	multiQC := wf.NewProc("create_multiqc_report", `export PYTHONPATH=../`+appsDir+`/MultiQC-1.5/lib/python2.7/site-packages:$PYTHONPATH && \
		../`+appsDir+`/MultiQC-1.5/bin/multiqc -f \
		-d ../`+tmpDir+`/rnaseqpre/ \
		-o $(o={o:report}; echo ${o%/multiqc_report.html}) # Depend: {i:count_features|join: }`)
	multiQC.In("count_features").From(featureCountS2SS.OutSubStream())
	multiQC.SetOut("report", tmpDir+"/rnaseqpre/multiqc/multiqc_report.html")

	// Handle missing flags
	procNames := []string{}
	for procName := range wf.Procs() {
		procNames = append(procNames, procName)
	}
	sort.Strings(procNames)
	//procNamesStr := strings.Join(procNames, "\n")
	//	sp.Error.Println("You must specify a process name pattern. You can specify one of:" + procNamesStr)
	//	flag.PrintDefaults()
	//	os.Exit(1)
	//}

	if *plot {
		dotFile := "rnaseqpre.dot"
		wf.PlotGraph(dotFile)
		fmt.Println("Wrote workflow graph to:", dotFile)
		return
	}
	wf.Run()
}

// fs is a short-hand for fmt.Sprintf(), to make string interpolation code less
// verbose and more readable
func fs(fmtString string, v ...interface{}) string {
	return fmt.Sprintf(fmtString, v...)
}
