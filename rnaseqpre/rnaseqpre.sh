

# set variables
tmpDir="tmp"
appsDir="data/apps"
refDir="$appsDir/pipeline_test/ref"
origDataDir="$appsDir/pipeline_test/data"
dataDir="data"




# FastQC
mkdir -p "$tmpDir/rnaseqpre/fastqc"
$appsDir/FastQC-0.11.5/fastqc "$origDataDir/SRR3222409_1.chr11.fq.gz" -o "$tmpDir/rnaseqpre/fastqc"
$appsDir/FastQC-0.11.5/fastqc "$origDataDir/SRR3222409_2.chr11.fq.gz" -o "$tmpDir/rnaseqpre/fastqc"



# STAR
mkdir -p "$tmpDir/rnaseqpre/star/"
$appsDir/STAR-2.5.3a/STAR --genomeDir "$refDir/rnaseq/star" --readFilesIn "$origDataDir/SRR3222409_1.chr11.fq.gz" "$origDataDir/SRR3222409_2.chr11.fq.gz" --runThreadN 16 --readFilesCommand zcat --outFileNamePrefix "$tmpDir/rnaseqpre/star/SRR3222409.chr11." --outSAMtype BAM SortedByCoordinate


# samtools
$appsDir/samtools-1.3.1/samtools index "$tmpDir/rnaseqpre/star/SRR3222409.chr11.Aligned.sortedByCoord.out.bam"


# QualiMap
mkdir -p "$tmpDir/rnaseqpre/qualimap/"
$appsDir/QualiMap-2.2/qualimap rnaseq -pe -bam $tmpDir/rnaseqpre/star/SRR3222409.chr11.Aligned.sortedByCoord.out.bam -gtf $refDir/rnaseq/Mus_musculus.GRCm38.92.gtf --outdir $tmpDir/rnaseqpre/qualimap/ --java-mem-size=4G > /dev/null 2>&1

# featureCounts
mkdir -p "$tmpDir/rnaseqpre/featurecounts/"
$appsDir/subread-1.5.2/featureCounts -p -a $refDir/rnaseq/Mus_musculus.GRCm38.92.gtf -t gene -g gene_id -s 0 -o "$tmpDir/rnaseqpre/featurecounts/tableCounts" $tmpDir/rnaseqpre/star/SRR3222409.chr11.Aligned.sortedByCoord.out.bam



# multiqc
$appsDir/MultiQC-1.5/bin/multiqc -f -d $tmpDir/rnaseqpre/ -o $tmpDir/rnaseqpre/multiqc

