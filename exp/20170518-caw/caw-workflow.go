package main

import (
	sp "github.com/scipipe/scipipe"
)

func main() {
	sp.InitLogInfo()

	// Set up paths
	// ------------------------------------------------
	// SCRATCHDIR=$(readlink -f $1)
	// APPSDIR=$(readlink -f $2)
	// REFDIR=$(readlink -f $3)
	// DATADIR=$(readlink -f $4)

	scratchDir := "tmp"
	appsDir := "data/apps"
	refDir := appsDir + "/pipeline_test/ref"
	origDataDir := appsDir + "/pipeline_test/ref"
	dataDir := "data"

	// ================================================================================
	// Initial data download workflow
	// ================================================================================

	// Init processes

	wf := sp.NewPipelineRunner()

	dlApps := sp.NewFromShell("dlApps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	dlApps.SetPathStatic("apps", dataDir+"/uppnex_apps.tar.gz")
	wf.AddProcess(dlApps)

	unzipApps := sp.NewFromShell("unzipApps", "zcat {i:targz} > {o:tar}")
	unzipApps.SetPathReplace("targz", "tar", ".gz", "")
	wf.AddProcess(unzipApps)

	untarApps := sp.NewFromShell("untarApps", "tar -xvf {i:tar} -C "+dataDir+" # {o:outdir}")
	untarApps.SetPathStatic("outdir", dataDir+"/apps")
	wf.AddProcess(untarApps)

	appsDirMultipl := NewFileMultiplicator(5)
	wf.AddProcess(appsDirMultipl)

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

	//alignSamples.SetPathStatic("reads1", dataDir+"/tiny_normal_L001_R1.fastq.gz")
	//alignSamples.SetPathStatic("reads2", dataDir+"/tiny_normal_L001_R2.fastq.gz")

	// ================================================================================
	// Main Workflow
	// ================================================================================

	// --------------------------------------------------------------------------------
	// Align Samples
	// --------------------------------------------------------------------------------
	refFasta := refDir + "/human_g1k_v37_decoy.fasta"
	refIndex := refDir + "/human_g1k_v37_decoy.fasta"

	// Define process
	alignSamples := sp.NewFromShell("alignSamples", "bwa mem -R \"@RG\tID:normal_{p:index}\tSM:normal\tLB:normal\tPL:illumina\" -B 3 -t 4 -M "+refFasta+" {i:reads1} {i:reads2}"+
		"| samtools view -bS -t "+refIndex+" - "+
		"| samtools sort - > {o:bam} # {i:appsdir}")

	// Create output file format
	alignSamples.PathFormatters["bam"] = func(t *sp.SciTask) string {
		outPath := scratchDir + "/normal_" + t.Params["index"] + ".bam"
		return outPath
	}
	wf.AddProcess(alignSamples)

	// Loop over indexes, and create parameters and file paths and send to alignSamples
	indexes := []string{"1", "2", "4", "7", "8"}
	reads1Paths := []string{}
	reads2Paths := []string{}
	for _, idx := range indexes {
		reads1Paths = append(reads1Paths, origDataDir+"/tiny_normal_L00"+idx+"_R1.fastq.gz")
		reads2Paths = append(reads2Paths, origDataDir+"/tiny_normal_L00"+idx+"_R2.fastq.gz")
	}

	reads1FileQueue := sp.NewIPQueue(reads1Paths...)
	wf.AddProcess(reads1FileQueue)

	reads2FileQueue := sp.NewIPQueue(reads1Paths...)
	wf.AddProcess(reads2FileQueue)

	readsIndexQueue := NewParamQueue(indexes...)
	wf.AddProcess(readsIndexQueue)

	// --------------------------------------------------------------------------------

	mainWfSink := sp.NewSink()
	wf.AddProcess(mainWfSink)

	// --------------------------------------------------------------------------------
	// Connect network
	// --------------------------------------------------------------------------------
	sp.Connect(dlApps.Out["apps"], unzipApps.In["targz"])
	sp.Connect(unzipApps.Out["tar"], untarApps.In["tar"])

	sp.Connect(untarApps.Out["outdir"], appsDirMultipl.In)
	sp.Connect(appsDirMultipl.Out, alignSamples.In["appsdir"])

	sp.Connect(reads1FileQueue.Out, alignSamples.In["reads1"])
	sp.Connect(reads2FileQueue.Out, alignSamples.In["reads2"])
	readsIndexQueue.Out.Connect(alignSamples.ParamPorts["index"])

	mainWfSink.Connect(alignSamples.Out["bam"])

	// --------------------------------------------------------------------------------
	// Run main workflow
	// --------------------------------------------------------------------------------
	wf.Run()
}

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
// echo -e "marking duplicates"
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
