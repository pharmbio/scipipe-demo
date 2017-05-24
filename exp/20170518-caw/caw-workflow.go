package main

import (
	sp "github.com/scipipe/scipipe"
	spcomp "github.com/scipipe/scipipe/components"
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

	// ================================================================================
	// Data Download part of the workflow
	// ================================================================================

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

	// ================================================================================
	// Main Workflow
	// ================================================================================

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

	// Create a map so that we can get an index number from the sample type, used in one of the processes
	markDupesOutputIndex := map[string]string{
		"normal": "0",
		"tumor":  "1",
	}
	for _, sampleType := range []string{"normal", "tumor"} {
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
				METRICS_FILE=`+tmpDir+`/`+sampleType+`_`+markDupesOutputIndex[sampleType]+`.md.bam \
				TMP_DIR=`+tmpDir+` \
				ASSUME_SORTED=true \
				VALIDATION_STRINGENCY=LENIENT \
				CREATE_INDEX=TRUE \
				OUTPUT={o:bam}; \
				mv `+tmpDir+`/`+sampleType+`_`+markDupesOutputIndex[sampleType]+`.md{.bam.tmp,}.bai;`)
		markDupes[sampleType].SetPathStatic("bam", tmpDir+"/"+sampleType+"_"+markDupesOutputIndex[sampleType]+".md.bam")
		markDupes[sampleType].GetInPort("bam").Connect(mergeBams[sampleType].GetOutPort("mergedbam"))
		wf.AddProcess(markDupes[sampleType])
	}

	// --------------------------------------------------------------------------------
	// Re-align Reads - Create Targets
	// --------------------------------------------------------------------------------
	// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T RealignerTargetCreator   -I normal_0.md.bam -I tumor_1.md.bam   -R $REFDIR/human_g1k_v37_decoy.fasta   -known $REFDIR/1000G_phase1.indels.b37.vcf   -known $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nt 4   -XL hs37d5   -XL NC_007605   -o tiny.intervals

	// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T IndelRealigner   -I normal_0.md.bam -I tumor_1.md.bam   -R $REFDIR/human_g1k_v37_decoy.fasta   -targetIntervals tiny.intervals   -known $REFDIR/1000G_phase1.indels.b37.vcf   -known $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -XL hs37d5   -XL NC_007605   -nWayOut '.real.bam'

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
			-nWayOut '.real.bam'; echo done > {o:realbam}`)
	realignIndels.GetInPort("intervals").Connect(realignCreateTargets.GetOutPort("intervals"))
	realignIndels.GetInPort("bamnormal").Connect(markDupesNormalFanOut.GetOutPort("realign_indels"))
	realignIndels.GetInPort("bamtumor").Connect(markDupesTumorFanOut.GetOutPort("realign_indels"))
	realignIndels.SetPathStatic("realbam", "realbam.done.txt")
	wf.AddProcess(realignIndels)

	mainWfSink.Connect(realignIndels.GetOutPort("realbam"))

	// --------------------------------------------------------------------------------
	// Re-calibrate reads
	// --------------------------------------------------------------------------------

	// java -Xmx3g   -Djava.io.tmpdir="$SCRATCHDIR/tmp"   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T BaseRecalibrator   -R $REFDIR/human_g1k_v37_decoy.fasta   -I normal_0.md.real.bam   -knownSites $REFDIR/dbsnp_138.b37.vcf   -knownSites $REFDIR/1000G_phase1.indels.b37.vcf   -knownSites $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nct 4   -XL hs37d5   -XL NC_007605   -l INFO   -o normal.recal.table
	// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T PrintReads   -R $REFDIR/human_g1k_v37_decoy.fasta   -nct 4   -I normal_0.md.real.bam   -XL hs37d5   -XL NC_007605   --BQSR normal.recal.table   -o normal.recal.bam
	//
	// java -Xmx3g   -Djava.io.tmpdir="$SCRATCHDIR/tmp"   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T BaseRecalibrator   -R $REFDIR/human_g1k_v37_decoy.fasta   -I tumor_1.md.real.bam   -knownSites $REFDIR/dbsnp_138.b37.vcf   -knownSites $REFDIR/1000G_phase1.indels.b37.vcf   -knownSites $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nct 4   -XL hs37d5   -XL NC_007605   -l INFO   -o tumor.recal.table
	// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T PrintReads   -R $REFDIR/human_g1k_v37_decoy.fasta   -nct 4   -I tumor_1.md.real.bam   -XL hs37d5   -XL NC_007605   --BQSR tumor.recal.table   -o tumor.recal.bam

	wf.AddProcess(mainWfSink)
	wf.Run()
}

// ========================================================================================================================
//
// Martin's original script below:
// #!/bin/bash
//
// # fail on errors
// set -e
//
// # save original PATH
// PATHBAK=$PATH
//
// # Added by Samuel, to make it run on UPPMAX:
// module load bioinfo-tools; module load bwa/0.7.15 samtools/1.4 GATK/3.7
//
// # devel, will be overwritten by the block below when run for reals
// SCRATCHDIR='/home/dahlo/cannyfs/apps/pipeline_test/scratch'
// APPSDIR='/home/dahlo/cannyfs/apps'
// REFDIR='/home/dahlo/cannyfs/apps/pipeline_test/ref'
// DATADIR='/home/dahlo/cannyfs/apps/pipeline_test/data'
//
// echo -e "Get arguemnts"
// SCRATCHDIR=$(readlink -f $1)
// APPSDIR=$(readlink -f $2)
// REFDIR=$(readlink -f $3)
// DATADIR=$(readlink -f $4)
//
// echo -e "create outdir etc"
// mkdir -p $SCRATCHDIR/tmp
// cd $SCRATCHDIR
//
// # set paths
// ulimit -n 10000  # only used by cannyfs, could be commented out when not benchmarking cannyfs
// export PATH=$PATH:$APPSDIR/nextflow:$APPSDIR/samtools/bin:$APPSDIR/vcftools_0.1.13/bin:$APPSDIR/tabix-0.2.6:$APPSDIR/strelka/bin:$APPSDIR/manta-1.0.3.centos5_x86_64/bin:$APPSDIR/bwa-0.7.15/
//
// # align samples
// echo -e "\naligning normal 1\n"
// bwa mem -R "@RG\tID:normal_1\tSM:normal\tLB:normal\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_normal_L001_R1.fastq.gz $DATADIR/tiny_normal_L001_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - | samtools sort - > $SCRATCHDIR/normal_1.bam
// echo -e "\naligning normal 2\n"
// bwa mem -R "@RG\tID:normal_2\tSM:normal\tLB:normal\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_normal_L002_R1.fastq.gz $DATADIR/tiny_normal_L002_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - | samtools sort - > $SCRATCHDIR/normal_2.bam
// echo -e "\naligning normal 4\n"
// bwa mem -R "@RG\tID:normal_4\tSM:normal\tLB:normal\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_normal_L004_R1.fastq.gz $DATADIR/tiny_normal_L004_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/normal_4.bam
// echo -e "\naligning normal 7\n"
// bwa mem -R "@RG\tID:normal_7\tSM:normal\tLB:normal\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_normal_L007_R1.fastq.gz $DATADIR/tiny_normal_L007_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/normal_7.bam
// echo -e "\naligning normal 8\n"
// bwa mem -R "@RG\tID:normal_8\tSM:normal\tLB:normal\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_normal_L008_R1.fastq.gz $DATADIR/tiny_normal_L008_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/normal_8.bam
//
// echo -e "\naligning tumor 1\n"
// bwa mem -R "@RG\tID:tumor_1\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L001_R1.fastq.gz $DATADIR/tiny_tumor_L001_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_1.bam
// echo -e "\naligning tumor 2\n"
// bwa mem -R "@RG\tID:tumor_2\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L002_R1.fastq.gz $DATADIR/tiny_tumor_L002_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_2.bam
// echo -e "\naligning tumor 3\n"
// bwa mem -R "@RG\tID:tumor_3\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L003_R1.fastq.gz $DATADIR/tiny_tumor_L003_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_3.bam
// echo -e "\naligning tumor 5\n"
// bwa mem -R "@RG\tID:tumor_5\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L005_R1.fastq.gz $DATADIR/tiny_tumor_L005_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_5.bam
// echo -e "\naligning tumor 6\n"
// bwa mem -R "@RG\tID:tumor_6\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L006_R1.fastq.gz $DATADIR/tiny_tumor_L006_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_6.bam
// echo -e "\naligning tumor 7\n"
// bwa mem -R "@RG\tID:tumor_7\tSM:tumor\tLB:tumor\tPL:illumina" -B 3 -t 4 -M $REFDIR/human_g1k_v37_decoy.fasta $DATADIR/tiny_tumor_L007_R1.fastq.gz $DATADIR/tiny_tumor_L007_R2.fastq.gz |   samtools view -bS -t $REFDIR/human_g1k_v37_decoy.fasta.fai - |   samtools sort - > $SCRATCHDIR/tumor_7.bam
//
// echo -e "\nmerging bams\n"
// samtools merge -f tumor.bam tumor_1.bam tumor_2.bam tumor_3.bam tumor_5.bam tumor_6.bam tumor_7.bam
// samtools merge -f normal.bam normal_1.bam normal_2.bam normal_4.bam normal_7.bam normal_8.bam
//
// WE ARE HERE --> echo -e "marking duplicates"
// java -Xmx15g   -jar $APPSDIR/picard-tools-1.118/MarkDuplicates.jar   INPUT=normal.bam   METRICS_FILE=normal.bam.metrics   TMP_DIR="$SCRATCHDIR/tmp"  ASSUME_SORTED=true   VALIDATION_STRINGENCY=LENIENT   CREATE_INDEX=TRUE   OUTPUT=normal_0.md.bam
// java -Xmx15g   -jar $APPSDIR/picard-tools-1.118/MarkDuplicates.jar   INPUT=tumor.bam   METRICS_FILE=tumor.bam.metrics   TMP_DIR="$SCRATCHDIR/tmp"   ASSUME_SORTED=true   VALIDATION_STRINGENCY=LENIENT   CREATE_INDEX=TRUE   OUTPUT=tumor_1.md.bam
//
// echo -e "realign reads"
// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T RealignerTargetCreator   -I normal_0.md.bam -I tumor_1.md.bam   -R $REFDIR/human_g1k_v37_decoy.fasta   -known $REFDIR/1000G_phase1.indels.b37.vcf   -known $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nt 4   -XL hs37d5   -XL NC_007605   -o tiny.intervals
// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T IndelRealigner   -I normal_0.md.bam -I tumor_1.md.bam   -R $REFDIR/human_g1k_v37_decoy.fasta   -targetIntervals tiny.intervals   -known $REFDIR/1000G_phase1.indels.b37.vcf   -known $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -XL hs37d5   -XL NC_007605   -nWayOut '.real.bam'
//
// echo -e "recalibrate reads"
// java -Xmx3g   -Djava.io.tmpdir="$SCRATCHDIR/tmp"   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T BaseRecalibrator   -R $REFDIR/human_g1k_v37_decoy.fasta   -I normal_0.md.real.bam   -knownSites $REFDIR/dbsnp_138.b37.vcf   -knownSites $REFDIR/1000G_phase1.indels.b37.vcf   -knownSites $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nct 4   -XL hs37d5   -XL NC_007605   -l INFO   -o normal.recal.table
// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T PrintReads   -R $REFDIR/human_g1k_v37_decoy.fasta   -nct 4   -I normal_0.md.real.bam   -XL hs37d5   -XL NC_007605   --BQSR normal.recal.table   -o normal.recal.bam
//
// java -Xmx3g   -Djava.io.tmpdir="$SCRATCHDIR/tmp"   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T BaseRecalibrator   -R $REFDIR/human_g1k_v37_decoy.fasta   -I tumor_1.md.real.bam   -knownSites $REFDIR/dbsnp_138.b37.vcf   -knownSites $REFDIR/1000G_phase1.indels.b37.vcf   -knownSites $REFDIR/Mills_and_1000G_gold_standard.indels.b37.vcf   -nct 4   -XL hs37d5   -XL NC_007605   -l INFO   -o tumor.recal.table
// java -Xmx3g   -jar $APPSDIR/gatk/GenomeAnalysisTK.jar   -T PrintReads   -R $REFDIR/human_g1k_v37_decoy.fasta   -nct 4   -I tumor_1.md.real.bam   -XL hs37d5   -XL NC_007605   --BQSR tumor.recal.table   -o tumor.recal.bam
//
// # restore path
// export PATH=$PATHBAK

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

// ================================================================================

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
