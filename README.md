# gabtk
Genome Assembly Benchmark Toolkit

Installation Instructions
================================
  
To install Genome Assembly Benchmark Toolkit, you must have a minimum of 6 GiB free disk space and minimum of 16 GiB free RAM to test run. 

To provide an easier way to install, we provide a miniconda based installer.
Installation also requires **pre-instaled** ``git``, ``gcc``, ``cpp`` and ``zlib1g-dev``.

    
    git clone https://github.com/sarangian/gabtk.git
    cd gabtk
    chmod 755 install.sh
    ./install.sh

    
**Post Installation Instructions**

	
After successful installation, close the current terminal. 
In a new terminal. source the bashrc file:  ``source ~/.bashrc``
Activate ``gabtk`` environment using command: ``conda activate`` 

List of commands
================

Prepare a Project for Benchmark Analysis
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The directory from which which the ``benchmark.py`` script will be fired, must contain the ``luigi.cfg`` template file


``luigi.cfg`` template file
----------------------------

.. code-block:: none


	[core]
	default-scheduler-port:0000
	error-email=

	[GlobalParameter]
	projectName=
	assembly_name=i
	projectDir=
	adapter=
	domain=
	pe_read_dir=
	mp_read_dir=
	ont_read_dir=
	pac_read_dir=
	pe_read_suffix=
	mp_read_suffix=
	genome_size=
	ont_read_suffix=
	pac_read_suffix=
	threads=
	maxMemory=
	seq_platforms=


configureProject
-----------------

.. code-block:: none


    benchmark.py configureProject --help


    benchmark.py configureProject --projectName  Illumina_PE_Benchmark \        
                                  --cpus 8 \
                                  --maxMemory 16 \
                                  --domain prokaryote \
                                  --dataDir  /home/user/Documents/genome_assembly_benchmark/data/ \
                                  --genomeName ecoli \
                                  --genomeSize 4600000 \
                                  --schedulerPort 8082 \
                                  --userEmail user@mail.com \
                                  --outDir raw_data_symlink \
                                  --local-scheduler

The   configureProject command will generate the (1) ``luigi.cfg`` file (2) outDir folder containg the symbolic link to the raw fastq files present in ``raw_data_symlink`` folder (3) ``sample_list`` folder containing 4 files (i) pe_samples.lst (ii)mp_samples.lst  (iii) ont_samples.lst  (iv) pac_samples.lst  


Example of a ``luigi.cfg`` file generated using configureProject command


.. code-block:: none

	[core]
	default-scheduler-port:8082
	error-email=a.n.sarangi@gmail.com

	[GlobalParameter]
	projectName=LongReadAssembly
	assembly_name=ecoli
	projectDir=/home/adityans/Documents/genome_assembly_benchmark/PEMP_Benchmark/
	adapter=/home/adityans/Documents/genome_assembly_benchmark/tasks/utility/adapters.fasta.gz
	domain=prokaryote
	pe_read_dir=/home/adityans/Documents/genome_assembly_benchmark/raw_data_symlink/pe/
	mp_read_dir=/home/adityans/Documents/genome_assembly_benchmark/raw_data_symlink/mp/
	ont_read_dir=/home/adityans/Documents/genome_assembly_benchmark/raw_data_symlink/ont/
	pac_read_dir=/home/adityans/Documents/genome_assembly_benchmark/raw_data_symlink/pacbio/
	pe_read_suffix=fq.gz
	mp_read_suffix=fq.gz
	genome_size=4600000
	ont_read_suffix=fq.gz
	pac_read_suffix=fastq.gz
	threads=4
	maxMemory=12
	seq_platforms=pe-pac



QC Analysis
^^^^^^^^^^^


rawReadsQC
-----------
.. code-block:: none


    benchmark.py rawReadsQC --help


    --seq-platforms     Choose From[pe: paired-end,
    					pe-mp: paired-end and mate-pair,
    					pe-ont: paired-end and nanopore, 
    					pe-pac: paired-end and pacbio, ont: nanopore, pac: pacbio]

                        Choices: {pe, pe-mp, pe-ont, pac, ont}

    Example 1: paired-end reads QC Analysis

    benchmark.py rawReadsQC \
    		--seq-platforms pe \
    		--local-scheduler

    Example 2: paired-end and mate-pair reads QC Analysis
    
    benchmark.py rawReadsQC \
    		--seq-platforms pe-mp \
    		--local-scheduler

    Example 3: paired-end and pacbio reads QC Analysis

    benchmark.py rawReadsQC \
    		--seq-platforms pe-pac \
    		--local-scheduler


cleanReads
-----------

.. code-block:: none
 
 	benchmark.py cleanReads <arguments> --local-scheduler

	 arguments            	  	type 	Description

	 Mandetory parameter

	 --seq_platforms      	  	str 	choose from [
 						pe: paired-end,
    						pe-mp: paired-end and mate-pair,
    						pe-ont: paired-end and nanopore, 
    						pe-pac: paired-end and pacbio, 
    						ont: nanopore,
    						pac: pacbio]

 	optional parameters

	 --cleanFastq-kmer-length	int     Kmer length used for finding contaminants.
                			Contaminants shorter than kmer length will not be found. 
                			Default 21
  	
	 --cleanFastq-corret-error  	Bool    Perform Error Correction Or Not
  					[Choose from True or False]
  					Default: False
  	
	 --cleanFastq-k-trim	    	str     Trimming protocol to remove bases matching 
  					reference kmers from reads
  					Choose From[f: dont trim, r: trim to right, l: trim to left]
  					Default: r

 	--cleanFastq-quality-trim  	str     Trimming protocol to remove bases with quality below the minimum
					average region quality from read ends. Performed after looking for kmers. If enabled, set also Average quality below which 					   to trim region.
					Choose From [f: trim neither end',
						rl: trim both end,
						r: trim only right end,
						l: trim only left end]
						Default: lr


 	--cleanFastq-min-GC		float   Discard reads with GC content below this. 
  					Default: min_gc=0.0

 	--cleanFastq-max-GC		float 	Discard reads with GC content below this. 
  					Default: max_gc=1.0

 	--cleanFastq-kmer-length	int     Kmer length used for finding contaminants. 
  					Default: kmer=13  

 	--cleanFastq-trim-front	int     Number of bases to be trimmed in front for read.
  					Default: 0

 	--cleanFastq-trim-tail 	int     trimming how many bases from the end of read.
  					Default: 0

 	--cleanFastq-max-n		int 	Maximum number of Ns after trimming
					If non-negative, reads with more Ns than this (after trimming) will be discarded.
					Default: -1

 	--cleanFastq-trim-quality  	int     Average quality below which to trim region
  					Default: 6

 	--cleanFastq-min-length    	int     Reads shorter than min_length will be discarded
  					Default: 40

	 --cleanFastq-min-average-quality	int 	Reads with average quality (after trimming) below this will be discarded
  						Default: 10

	 --cleanFastq-min-base-quality 		int 	Reads with any base below this quality (after trimming) will be discarded
  						Default: 0
	 --cleanFastq-long-read-min-length	int	This parameter is specific for long read (pacbio / nanopore )only 
						seq_platforms='pac or ont']. 
						Reads shorter than min_length will be discarded. 				
						Default: long_read_min_length=1000 

	 --cleanFastq-long-read-mean-quality   int  	This parameter is specific for long read only.
							The mean quality is the mean read identity as indicated by the Phred quality scores. Example:
							example, consider a read where all the fastq quality characters are +. The qscores for each base are 10 							whichequates to a 90 percentage chance of being correct. This read would then have a mean quality score of 							   90. Read mean qualities are converted to a z-score and scaled to the range 0-100 to make the mean qality 							   score. This means  that the read with the worst mean quality in the input set will get a mean quality score 							      of 0 and the read with the best mean quality will get a mean quality score of 100. Default:  
							meanQ=80

 	--cleanFastq-long-read-keep-percent   int  	This parameter is specific for long read only.
  							The percentage of the best reads to be retained. 
  							Default: keep_percent=90
	 --local-scheduler



correctPAC
----------

.. code-block:: none


	benchmark.py correctPAC <arguments> --local-scheduler

	Mandetory Arguments
	--pre-process-reads   str   Choose [yes or no]


	Example

	benchmark.py correctPAC --pre-process-reads yes --local-scheduler



correctONT
----------

.. code-block:: none


	Mandetory Arguments
	--pre-process-reads   str   Choose [yes or no]

	benchmark.py correctONT --pre-process-reads yes --local-scheduler


Genome Assembly
^^^^^^^^^^^^^^^

skesa
-----

.. code-block:: none

 	Note: skesa is used for assembling prokaryotic Illumina paired-end reads only


 	Mandetory Arguments
 	--pre-process-reads   	str   	Choose [yes or no]

	 Optional Argument
	 --kmer 		int 	Minimal Kmer length for assembly
				Default: 21
 	--steps 		int     Number of assembly iterations from minimal to maximal kmer length in reads
                		Default: 11

	 --min-contig-length	int 	Exclude contigs from the FASTA file which are shorter than this length. 
                		Default: 200

**Example: run skesa assembler**

.. code-block:: none

	case 1: running skesa with out read cleaning

	benchmark.py skesa --pre-process-read no --min-contig-length 500 --local-scheduler

	
	case 2: running skesa with read cleaning parameters

	benchmark.py skesa \
			--pre-process-read yes \
			--min-contig-length 500 \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

lightAssembler
---------------

.. code-block:: none

	 Note: lightAssembler is used for assembling Illumina paired-end reads only

	 Mandetory Arguments
 	--pre-process-reads   	str   	Choose [yes or no]

	 Optional Argument
	 --kmer      		int 	Minimal Kmer length for assembly


	**Example: run light assembler**

.. code-block:: none

	case 1: running lightassembler with out read cleaning

	benchmark.py lightAssembler --pre-process-read no --local-scheduler

	
	case 2: running lightassembler with read cleaning parameters

	benchmark.py lightAssembler \
			--pre-process-read yes \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

discovardenovo
---------------

.. code-block:: none

 Note: discovardenovo is used for assembling Illumina paired-end reads only

 Mandetory Arguments
 --pre-process-reads	str   	Choose [yes or no]

 Optional Argument
 --kmer 		int 	Minimal Kmer length for assembly

 **Example: run discovardenovo assembler**

.. code-block:: none

	case 1: running discovardenovo with out read cleaning

	benchmark.py discovardenovo --pre-process-read no --local-scheduler

	
	case 2: running discovardenovo with read cleaning parameters

	benchmark.py discovardenovo \
			--pre-process-read yes \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler


sparseAssembler
---------------

.. code-block:: none

	Note: sparseAssembler is used for assembling Illumina paired-end or paired-end with mate-pair reads only


	Mandetory Arguments
	--pre-process-reads   	str   	Choose [yes or no]
	--seq-platforms      	str   	Choose [pe:paired-end, pe-mp: paired-end and mate-pair]

	Optional Argument
	--kmer 			int 	Minimal Kmer length for assembly

**Example: run sparse assembler**

.. code-block:: none

	case 1: running sparseAssembler with out read cleaning

	benchmark.py sparseAssembler --pre-process-read no --local-scheduler

	
	case 2: running sparseAssembler with read cleaning parameters

	benchmark.py sparseAssembler \
			--pre-process-read yes \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler


spades
-------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  str   	Choose [yes or no]

 --seq-platforms      str   	Choose [pe:paired-end, 
				pe-mp: paired-end and mate-pair
				pe-ont: paired-end and nanopore
				pe-pac: paired-end and pacbio]

 Optional Argument
 --kmer 		str 	comma separated list of kmers. must be odd and less than 128. 
                        	Default: auto

 --cov-cutoff 		str    	coverage cutoff value (a positive float number, or 'auto', or 'off'). 
    				Default 'off'


**Example: run spades assembler**

.. code-block:: none

	case 1: running spades with out read cleaning

	benchmark.py spades --pre-process-read no  --spades-seq-platforms pe-pac --local-scheduler

	
	case 2: running spades with read cleaning parameters

	benchmark.py spades \
			--pre-process-read yes \
			--spades-seq-platforms pe-pac \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

unicycler
---------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads str   	Choose [yes or no]

 --seq-platforms     str   	Choose [pe:paired-end, 
				pe-ont: paired-end and nanopore
				pe-pac: paired-end and pacbio]

 Optional Argument
 --mode 		str 	Choose From[
 				normal: moderate contig size and misassembly rate
 				conservative: smaller contigs lowest misassembly rate
                   		bold: longest contigs higher misassembly rate]

 --min-contig-length	int 	Exclude contigs from the FASTA file which are shorter
                        	than this length. 
                        	Default: 200

**Example: run unicycler assembler**

.. code-block:: none

	case 1: running unicycler with out read cleaning

	benchmark.py unicycler --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running unicycler with read cleaning parameters

	benchmark.py unicycler \
			--pre-process-read yes \
			--seq-platforms pe-pac \
			--mode normal \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler


masurca
-------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  	str   	Choose [yes or no]

 --seq-platforms        str   	Choose [pe:	paired-end, 
					pe-mp:	paired-end and mate-pair
					pe-ont: paired-end and nanopore
					pe-pac: paired-end and pacbio]

 --pe-frag-mean   	float   	Illumina paired-end fragment mean

 --pe-frag-sd	  	float   	Illumina paired-end fragment sd

 --mp-frag-mean   	float   	Illumina mate-pair reads mean
 					optional --in case of paired-end read only assembly

 --mp-frag-sd     	float   	lumina mate-pair reads sd
 					optional --in case of paired-end read only assembly
							
**Example: run masurca assembler**

.. code-block:: none
	
	NOTE: currently running masurca assembler through the gabtk pipeline supports only one paired-end and (or)
	only one mate-pair library. i.e multipe read libraries are not supported

	masurca assembler does not require read cleaning.

	Case 1: running masurca assembler with one paired-end read library and one pacbio read library

	
	benchmark.py masurca \
			--seq-platforms pe-pac \
			--pe-frag-mean 180 \
			--pe-frag-sd 20	\
			--local-scheduler

ray
--------
.. code-block:: none

 Mandetory Arguments
 --pre-process-reads str   	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pe:	paired-end
				pe-mp:	paired-end and mate-pair
				pe-ont:	paired-end and nanopore
				pe-pac:	paired-end and pacbio]


**Example: run ray assembler**

.. code-block:: none

	case 1: running ray with out read cleaning

	benchmark.py ray --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running ray with read cleaning parameters

	benchmark.py ray \
			--pre-process-read yes \
			--seq-platforms pe-pac \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

idba
---------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pe:	paired-end
				pe-mp:	paired-end and mate-pair
				pe-ont:	paired-end and nanopore
				pe-pac:	paired-end and pacbio]

**Example: run idba assembler**

.. code-block:: none

	case 1: running idba with out read cleaning

	benchmark.py idba --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running idba with read cleaning parameters

	benchmark.py idba \
			--pre-process-read yes \
			--seq-platforms pe-pac \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

abyss
---------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  str   Choose [yes or no]

 --seq-platforms	str Choose [
 				pe:	paired-end
				pe-mp:	paired-end and mate-pair
				pe-ont:	paired-end and nanopore
				pe-pac:	paired-end and pacbio]

**Example: run abyss assembler**

.. code-block:: none

	case 1: running abyss with out read cleaning

	benchmark.py abyss --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running abyss with read cleaning parameters

	benchmark.py abyss \
			--pre-process-read yes \
			--seq-platforms pe-pac \
			--cleanFastq-min-average-quality 20 \
			--cleanFastq-long-read-mean-quality 70 \
			--local-scheduler

haslr
---------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  str   Choose [yes or no]

 --seq-platforms	str Choose [
 				pe-ont: paired-end and nanopore
				pe-pac: paired-end and pacbio]


**Example: run haslr assembler**

.. code-block:: none

	case 1: running haslr with out read cleaning

	benchmark.py haslr --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running haslr with read cleaning parameters

	benchmark.py haslr \
			--pre-process-read yes \
			--seq-platforms pe-pac \
			--cleanFastq-min-average-quality 20 \
			--cleanFastq-long-read-mean-quality 70 \
			--local-scheduler

soapdenovo
-----------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  str   Choose [yes or no]

 --seq-platforms      str   Choose [
 				pe:	paired-end
				pe-mp: paired-end and mate-pair]

 --max-read-len       int     Maximum read length
 --avg-pe-ins         float   paired-end reads average insert size
 --avg-me-ins	      float   mate-paired reads average insert size
 --min-contig-length  int     minimum contig length

**Example: run soapdenovo assembler**

.. code-block:: none

	case 1: running soapdenovo with out read cleaning

	benchmark.py soapdenovo --pre-process-read no  --seq-platforms pe-pac --local-scheduler

	case 2: running soapdenovo with read cleaning parameters (read library: paired-end)

	benchmark.py soapdenovo \
			--pre-process-read yes \
			--seq-platforms pe \
			--cleanFastq-min-average-quality 20 \
			--max-read-len 150 \
			--avg-pe-ins 180 \
			--min-contig-length 200 \
			--local-scheduler



dbg2olc
--------

.. code-block:: none

 Mandetory Arguments
 --pre-process-reads  str   	Choose [yes or no]

 --seq-platforms      str   	Choose [
 				ont: nanopore
 				pac: pacbio]

 Optional Arguments
 --dbg-assembler	  str   Choose [
 				light: 	LightAssembler
 				sparse: SparseAssembler
 				minia:  MiniaAssembler]	
 				Default: sparse

**Example: run dbg2olc assembler**

.. code-block:: none

	case 1: running dbg2olc with out read cleaning

	benchmark.py dbg2olc --pre-process-read no  --seq-platforms pac --local-scheduler

	case 2: running dbg2olc with read cleaning parameters 

	benchmark.py dbg2olc \
			--pre-process-read yes \
			--seq-platforms pac \
			--cleanFastq-min-average-quality 20 \
			--dbg-assembler sparse \
			--local-scheduler



smartdenovo
------------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platform	str   	Choose [
 				ont: nanopore
				pac: pacbio]
 Optional Arguments
 --min-contig-size	int	Default: 500


 benchmark.py smartdenovo \
			--pre-process-read yes \
			--seq-platforms pac \
			--cleanFastq-min-average-quality 20 \
			--local-scheduler

flye
-----


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pacbio-raw: pacbio raw
				pacbio-corr: pacbio corrected
				nano-raw: nanopore raw
				nano-corr: nanopore corrected]

**Example: run flye assembler**

.. code-block:: none

	case 1: running flye with out read cleaning

	benchmark.py flye --pre-process-read no  --seq-platforms pacbio-raw --local-scheduler

	case 2: running flye with read cleaning parameters 

	benchmark.py flye \
			--pre-process-read yes \
			--seq-platforms pacbio-raw \
			--local-scheduler


canu
------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pacbio-raw: pacbio raw
				pacbio-corr: pacbio corrected
				nano-raw: nanopore raw
				nano-corr: nanopore corrected]
 

**Example: run canu assembler**

.. code-block:: none

	case 1: running canu with out read cleaning

	benchmark.py canu --pre-process-read no  --seq-platforms pacbio-raw --local-scheduler

	case 2: running canu with read cleaning parameters 

	benchmark.py canu \
			--pre-process-read yes \
			--seq-platforms pacbio-raw \
			--local-scheduler

mecat2
------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 Optional Argument
 --seq-platform	str   	Default: pac
 
**Example: run mecat2 assembler**
**Note: Read type must be pacbio**

.. code-block:: none

	case 1: running mecat2 with out read cleaning

	benchmark.py mecat2 --pre-process-read no --local-scheduler

	case 2: running mecat2 with read cleaning parameters 

	benchmark.py mecat2 \
			--pre-process-read yes \
			--local-scheduler
necat
-------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads		strs	Choose [yes or no]

 Optional Argument
 --seq-platform	str   	Default: ont
 
**Example: run necat assembler**
**Note: Read type must be nanopore**

.. code-block:: none

	case 1: running necat with out read cleaning

	benchmark.py necat --pre-process-read no --local-scheduler

	case 2: running necat with read cleaning parameters 

	benchmark.py neact \
			--pre-process-read yes \
			--local-scheduler

abruijn
---------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platform	str   	Choose [
 				pac: pacbio 
				ont: nanopore]

**Example: run abruijn assembler**

.. code-block:: none

	case 1: running abruijn with out read cleaning

	benchmark.py abruijn --pre-process-read no --seq-platform pacbio --local-scheduler

	case 2: running abruijn with read cleaning parameters 

	benchmark.py abruijn \
			--seq-platform pacbio \
			--pre-process-read yes \
			--local-scheduler


wtdbg2
--------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pac: pacbio 
				ont: nanopore]

**Example: run abruijn assembler**

.. code-block:: none

	case 1: running wtdbg2 with out read cleaning

	benchmark.py wtdbg2 --pre-process-read no --seq-platform pac --local-scheduler

	case 2: running abruijn with read cleaning parameters 

	benchmark.py wtdbg2 \
			--seq-platform pac \
			--pre-process-read yes \
			--local-scheduler
miniasm
--------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				pac: pacbio 
				ont: nanopore]

**Example: run abruijn assembler**

.. code-block:: none

	case 1: running miniasm with out read cleaning

	benchmark.py miniasm --pre-process-read no --seq-platform pac --local-scheduler

	case 2: running miniasm with read cleaning parameters 

	benchmark.py miniasm \
			--seq-platform pac \
			--pre-process-read yes \
			--local-scheduler

falcon
-------


.. code-block:: none

 Mandetory Arguments
 --pre-process-reads	strs	Choose [yes or no]

 --seq-platforms	str   	Choose [
 				nanopore
				pacbio]
 Optional Arguments
 --min-contig-length	int	Default: 500

