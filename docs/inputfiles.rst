
.. _inputfiles:

Input Files
===========

Raw Illumina short reads and (or) pacbio/nanopore long reads
------------------------------------------------------------

|   The illumina short reads and (or) pacbio/nanopore long reads in ``FASTQ`` format must be placed inside a folder with read permission
|   Allowed ``extension`` for FASTQ reads: ``fq`` , ``fq.gz`` , ``fastq``, ``fastq.gz``
| 
|   For **paired-end**  reads, sample name must be suffixed with _R1. ``extension`` and _R2. ``extension`` for forward and reverse reads respectively
|         
|	*Example*
|           sample_x_R1.fastq.gz 
|           sample_x_R2.fastq.gz
|           sample_y_R1.fastq.gz 
|           sample_y_R2.fastq.gz 
|           sample_z_R1.fastq.gz 
|           sample_z_R1.fastq.gz 
|                 
|   where  ``sample_x`` , ``sample_y`` , ``sample_z`` , are the sample names
|           sample_name must be suffixed with _R1.{extension} and _R2.{extension}
| 
|     
|   For **long reads** sample name must be suffixed with ``.{extension}``
|
|          *Example*
|           sample_x.fastq.gz
|           sample_y.fq.gz
|           sample_z.fastq
|
