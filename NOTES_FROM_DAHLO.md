dahlo wrote, 2017-05-08 (on pharmbio slack):

[11:15]: ett mail jag skickade till någon exjobbare ola har, som skulle köra samma del

> Ok, nu kan du ladda ner http://uppnex.se/apps.tar.gz , sen borde det bara vara att köra igång det:
> Syntax:
> 
> ```bash
> flexible_location_pipeline.sh <WORK DIR> <APPS DIR> <REF DIR> <INPUTDATA DIR>
> ```
> 
> Example run:
> 
> ```bash
> cd apps/pipeline_test/
> bash flexible_location_pipeline.sh scratch/ ../ ref/ data/
> ```

> Ah, ta bort rad 39 (står bara ett exit på den raden) i
> flexible_location_pipeline.sh så funkar det bättre. Jag la in ett exit där
> för att kolla att programmet ens kommer igång. Det blir ingen vidare pipeline
> om det är kvar dock "

Jag går igenom stegen nedan:

Steg 1: alignment (rad 35-60) Görs på varje prov separat, alltså 16
individuella alignments. Dessa kan spridas ut på flera datorer, tex om man har
16 datorer så kan man lägga ett prov på varje. Alignments i sig kan
parallelliseras inom en dator med multithreading som är inbyggt i programmet
man kör (bwa). Man säger åt bwa hur många trådar man ska köra med genom att ge
den -t n  där n är antalet man vill ha (ex. bwa mem -t 4 ......  för att köra
det med 4 trådar). Resultatet av en alignment är en bam-fil.

Steg 2: slå ihop proverna till en bam-fil per grupp. (rad 64-67) Nu är alla
alignade prover i varsin bam-fil, dvs 16 st. Det är lite opraktiskt att jobba
med, så ofta slår man ihop dem till en bam-fil per grupp. Det görs med samtools
merge. Det blir en körning per grupp, och dessa kan man lägga på varsin dator
om man vill snabba upp saker. Man kan även multithreada programmet i sig genom
att ge samtools -@ n (ex. samtools merge -@ 4 .....  för 4 trådar) vilket
snabbar upp det en del. Såg nu att -@ inte är angett i flexible_pi.... så det
kan du ju lägga till om du vill. Kommer nog inte öka hastigheten med 4x dock.
Det är antalet trådar man komprimerar med man ställer in, och komprimeringen är
ganska billig jämfört med att sortera reads och skriva till disk. Skadar inte
att ha med dock

Steg 3: mark duplicates (rad 70-73) Detta steg har jag för mig hittar reads som
är identiska och tar bort de extra kopiorna. Det kan ha skapats genom bias i
sekvenseringsmaskinen eller i något och preprocessningssstegen som pcr etc.
Körs oxå en gång per grupp och man kan lägga varje körning på en egen dator.
Programmet i sig kan multithreadas med -nt n.

Steg 4: realign reads (rad 77-80) Här går man in och gör om alignment runt
kända snps och indels. Detta för att den första alignment man gör är gjord för
att vara snabb, men göra något fler fel. Att finlira kostar betydligt mer
beräkningskraft så det göra man bara i regioner man vet kan vara svåra, tex där
man vet att det finns många snps och indels. Detta steg körs bara en gång med
båda proverna som input (vet inte om det blir skillnad av att köra dem en gång
per prov separat). Dvs man kör det bara på en dator, men det kan multitrådas
med -nt n.

Steg 5: recalibrate reads (rad 83-90) Detta steg görs för att justera om de
kvalitetspoäng sekvenseringsmaskinen har gett varje bas den sekvenserar. Det är
ett mått på hur säker maskinen var på att den läste rätt. Nu när alignment är
gjord så kan man kolla hur rätt maskinen faktiskt var (man antar att alignment
är sann) och man kan justera om kvaliteten på baserna. Lite hokuspokus, men det
är så folk gör. Det görs en gång per grupp, så man kan lägga det på två olika
datorer. Kan multitrådas med -nct n.

Så, det var nog alla steg. Så svaret är att en del steg går att parallelliseras
på flera datorer, och alla steg går att multitråda på en enskild dator.

[11:16] 
så, det borde vara allt

[11:16] 
det är preprocessningsstegen för CAW-pipelinene

[11:16] 
det är vad de kör för att testa att allt är installerat som det ska

[11:16] 
tar typ 20 min att köra
