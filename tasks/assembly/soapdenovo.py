#!/usr/bin/env python3

##################################################################
#
#
##################################################################

import os
import re
from tasks.assembly.kmergenie import kmergenie_formater_cleanFastq
from tasks.assembly.kmergenie import kmergenie_formater_reformat
from tasks.assembly.kmergenie import optimal_kmer

from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.reFormatReads import reformat


import luigi
import os
import subprocess

class GlobalParameter(luigi.Config):
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	domain=luigi.Parameter()
	assembly_name=luigi.Parameter()
	genome_size=luigi.Parameter()

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output

def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
	except OSError:
		print ('Error: Creating directory. ' + directory)



class soapdenovo(luigi.Task):
	projectName = GlobalParameter().projectName
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	assembly_name=GlobalParameter().assembly_name
	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end','pe-mp: paired-end and mate-pair']",
										  choices=["pe","pe-mp"], var_type=str)

	max_read_len = luigi.Parameter(description="Maximum Read Length")
	avg_pe_ins = luigi.Parameter(description="Average paired-end Read Insert size")
	avg_me_ins = luigi.Parameter(description="Average mate-pair Read Insert size",default='')

	min_contig_length = luigi.Parameter(description="minimum contig length",default='500')

	genome_size=GlobalParameter().genome_size

	def requires(self):
		if self.seq_platforms == "pe" and self.pre_process_reads =="yes":
			return [[cleanFastq(seq_platforms=self.seq_platforms,
						   sampleName=i)
					 for i in [line.strip()
							   for line in
							   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

					[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],
					]

		if self.seq_platforms == "pe" and self.pre_process_reads =="no":
			return [[reformat(seq_platforms=self.seq_platforms,
						   sampleName=i)
					 for i in [line.strip()
							   for line in
							   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

					[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],
					]

		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
			return [
				[cleanFastq(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[cleanFastq(seq_platforms="mp", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
			]

		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
			return [
				[reformat(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[reformat(seq_platforms="mp", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
			]


	def output(self):
		soapdenovo_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "SOAPDeNoVo" , self.assembly_name+ "/")
		return {'out': luigi.LocalTarget(soapdenovo_assembly_folder + "final.genome.scf.fasta")}

	def run(self):
		soapdenovo_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "SOAPDeNoVo" , self.assembly_name + "/")
		
		soapdenovo_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "log", "GenomeAssembly", "SOAPDeNoVo" +  "/")

		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
		
		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")

		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		cleaned_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "MP-Reads" + "/")

		
		def soapdenovo_illumina(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				pe_cleaned_read_folder = inputDir

				left_reads_list=[pe_cleaned_read_folder + x for x in left_read_name_list ]
				right_reads_list=[pe_cleaned_read_folder + x for x in right_read_name_list]
				left_reads='\n'.join(left_reads_list) 
				right_reads='\n'.join(right_reads_list) 

				return left_reads,right_reads

		
		config_folder_path=os.path.join(os.getcwd(), "configuration")
		soapdenovo_config_path=os.path.join(os.getcwd(), "configuration", "soapdenovo.cfg")
		createFolder(config_folder_path)

		if self.seq_platforms == "pe":
			with open (soapdenovo_config_path,'w') as configfile:
				configfile.write('maximal read length\n')
				configfile.write('max_rd_len={max_read_len}\n'.format(max_read_len=self.max_read_len))

				configfile.write('[LIB]\n')
				configfile.write('#average insert size\n')
				configfile.write('avg_ins={avg_pe_ins}\n'.format(avg_pe_ins=self.avg_pe_ins))

				configfile.write('##if sequence needs to be reversed\n')
				configfile.write('reverse_seq=0\n')

				configfile.write('##in which part(s) the reads are used\n')
				configfile.write('asm_flags=3\n')


				configfile.write('#in which order the reads are used while scaffolding\n')
				configfile.write('rank=1\n')


				configfile.write('# cutoff of pair number for a reliable connection (at least 3 for short insert size)\n')
				configfile.write('pair_num_cutoff=3\n')


				configfile.write('#minimum aligned length to contigs for a reliable read location (at least 32 for short insert size)\n')
				configfile.write('map_len=32\n')

				configfile.write('#a pair of fastq file, read 1 file should always be followed by read 2 file\n')


				if self.seq_platforms == "pe" and self.pre_process_reads =="no":
					pe_left_reads,pe_right_reads = soapdenovo_illumina(pe_sample_list, verified_pe_read_folder)
					configfile.write('q1={pe_left_reads}\n'.format(pe_left_reads=pe_left_reads))
					configfile.write('q2={pe_right_reads}\n'.format(pe_right_reads=pe_right_reads))

				if self.seq_platforms == "pe" and self.pre_process_reads =="yes":
					pe_left_reads,pe_right_reads = soapdenovo_illumina(pe_sample_list, cleaned_pe_read_folder)
					configfile.write('q1={pe_left_reads}\n'.format(pe_left_reads=pe_left_reads))
					configfile.write('q2={pe_right_reads}\n'.format(pe_right_reads=pe_right_reads))

		if self.seq_platforms == "pe-mp":
			with open (soapdenovo_config_path,'w') as configfile:
				configfile.write('maximal read length\n')
				configfile.write('max_rd_len={max_read_len}\n'.format(max_read_len=self.max_read_len))


				configfile.write('\n')
				configfile.write('[LIB]\n')
				configfile.write('#average insert size\n')
				configfile.write('avg_ins={avg_pe_ins}\n'.format(avg_pe_ins=self.avg_pe_ins))

				configfile.write('##if sequence needs to be reversed\n')
				configfile.write('reverse_seq=0\n')

				configfile.write('##in which part(s) the reads are used\n')
				configfile.write('asm_flags=3\n')


				configfile.write('#in which order the reads are used while scaffolding\n')
				configfile.write('rank=1\n')


				configfile.write('# cutoff of pair number for a reliable connection (at least 3 for short insert size)\n')
				configfile.write('pair_num_cutoff=3\n')


				configfile.write('#minimum aligned length to contigs for a reliable read location (at least 32 for short insert size)\n')
				configfile.write('map_len=32\n')

				configfile.write('#a pair of fastq file, read 1 file should always be followed by read 2 file\n')


				if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
					pe_left_reads,pe_right_reads = soapdenovo_illumina(pe_sample_list, verified_pe_read_folder)
					configfile.write('q1={pe_left_reads}\n'.format(pe_left_reads=pe_left_reads))
					configfile.write('q2={pe_right_reads}\n'.format(pe_right_reads=pe_right_reads))

				if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
					pe_left_reads,pe_right_reads = soapdenovo_illumina(pe_sample_list, cleaned_pe_read_folder)
					configfile.write('q1={pe_left_reads}\n'.format(pe_left_reads=pe_left_reads))
					configfile.write('q2={pe_right_reads}\n'.format(pe_right_reads=pe_right_reads))

				configfile.write('\n')
				configfile.write('[LIB]\n')
				configfile.write('#average insert size\n')
				configfile.write('avg_ins={avg_me_ins}\n'.format(avg_me_ins=self.avg_me_ins))
				configfile.write('reverse_seq=1\n')
				configfile.write('asm_flags=2\n')
				configfile.write('pair_num_cutoff=5\n')
				configfile.write('map_len=35\n')
				configfile.write('rank=2\n')

				if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
					mp_left_reads,mp_right_reads = soapdenovo_illumina(mp_sample_list, verified_mp_read_folder)
					configfile.write('q1={mp_left_reads}\n'.format(mp_left_reads=mp_left_reads))
					configfile.write('q2={mp_right_reads}\n'.format(mp_right_reads=mp_right_reads))

				if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
					mp_left_reads,mp_right_reads = soapdenovo_illumina(mp_sample_list, cleaned_mp_read_folder)
					configfile.write('q1={mp_left_reads}\n'.format(mp_left_reads=mp_left_reads))
					configfile.write('q2={mp_left_reads}\n'.format(mp_left_reads=mp_left_reads))
			
				configfile.close()
			
		print("Generated the SOAPdenovo Config File at {path}".format(path=soapdenovo_config_path))

		print('')
		print('')

		kmer = optimal_kmer((os.path.join(os.getcwd(),self.projectName, "GenomeAssembly", "KmerGenie", "kmergenni_pe.lst")))


		soapdenovo_run_cmd = "[ -d  {soapdenovo_assembly_folder} ] || mkdir -p {soapdenovo_assembly_folder}; mkdir -p {soapdenovo_assembly_log_folder};" \
									"cd {soapdenovo_assembly_folder}; " \
									"/usr/bin/time -v SOAPdenovo-127mer all " \
									"-s {soapdenovo_config_path} " \
									"-p {threads} " \
									"-K {kmer} " \
									"-L {min_contig_length} " \
									"-N {genome_size} " \
									"-o {assembly_name}_out 2>&1 | tee {soapdenovo_assembly_log_folder}soapdenovo.log" \
									.format(soapdenovo_assembly_folder=soapdenovo_assembly_folder,
											soapdenovo_assembly_log_folder=soapdenovo_assembly_log_folder,
											soapdenovo_config_path=soapdenovo_config_path,
											threads=GlobalParameter().threads,
											kmer=kmer,
											min_contig_length=self.min_contig_length,
											genome_size=GlobalParameter().genome_size,
											assembly_name=GlobalParameter().assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + soapdenovo_run_cmd)
		run_cmd(soapdenovo_run_cmd)