#!/usr/bin/env python3
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
	pe_read_dir=luigi.Parameter()
	mp_read_dir=luigi.Parameter()
	ont_read_dir=luigi.Parameter()
	pac_read_dir=luigi.Parameter()
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



class masurca(luigi.Task):
	projectName = GlobalParameter().projectName
	#pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	assembly_name=GlobalParameter().assembly_name

	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end',pe-mp: paired-end and mate-pair, pe-ont: paired-end and nanopore, pe-pac: paired-end and pacbio ']",
										  choices=["pe","pe-mp","pe-ont","pe-pac"], var_type=str)

	pe_frag_mean=luigi.Parameter()
	pe_frag_sd=luigi.Parameter()

	mp_frag_mean=luigi.Parameter(default='')
	mp_frag_sd=luigi.Parameter(default='')

	mega_read_assembler = luigi.ChoiceParameter(default="CABOG",choices=['CABOG','FLYE'], var_type=str)
	genome_size=GlobalParameter().genome_size

	def requires(self):
		

		if self.seq_platforms == "pe":
			return [reformat(seq_platforms="pe",sampleName=i) 
							for i in [line.strip() 
								for line in open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]
					]
			
		if self.seq_platforms == "pe-mp":
			return [
						[reformat(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[reformat(seq_platforms="mp",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
					]

		if self.seq_platforms == "pe-ont":
			return [
						[reformat(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[reformat(seq_platforms="ont",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
					]

		if self.seq_platforms == "pe-pac":
			return [
						[reformat(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[reformat(seq_platforms="pac",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
					]




	def output(self):
		masurca_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MaSuRCA" , self.assembly_name, "CA" + "/")
		return {'out': luigi.LocalTarget(masurca_assembly_folder + "final.genome.scf.fasta")}

	def run(self):
		masurca_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MaSuRCA", self.assembly_name + "/")
		
		masurca_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "log", "GenomeAssembly", "MaSuRCA" +  "/")

		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
		ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")



		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")
		verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
		verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

		
		def masurca_illumina(samplefile,inputDir):
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

		def masurca_longread(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fastq'
				read_name_list = [x + read_name_suffix for x in sample_name_list]
				input_read_folder = inputDir
				reads_list = [input_read_folder + x for x in read_name_list]
				long_reads = ' '.join(reads_list)
				return long_reads


		config_folder_path=os.path.join(os.getcwd(), "configuration")
		masurca_config_path=os.path.join(os.getcwd(), "configuration", "masurca.cfg")
		createFolder(config_folder_path)

		GS = int(self.genome_size) 
		JFSIZE = GS*20

		if self.mega_read_assembler=="CABOG":
			assembler="0"
		if self.mega_read_assembler=="FLYE":
			assembler="1"

		
		with open (masurca_config_path,'w') as configfile:
			configfile.write('DATA\n')
			if self.seq_platforms=="pe":

				pe_left_reads,pe_right_reads = masurca_illumina(pe_sample_list, verified_pe_read_folder)

				configfile.write('PE= pe {pe_frag_mean} {pe_frag_sd} {pe_left_reads} {pe_right_reads}\n'.format(pe_frag_mean = self.pe_frag_mean, pe_frag_sd = self.pe_frag_sd, pe_left_reads = pe_left_reads, pe_right_reads = pe_right_reads))
			
			if self.seq_platforms=="pe-mp":

				pe_left_reads,pe_right_reads = masurca_illumina(pe_sample_list, verified_pe_read_folder)
				mp_left_reads,mp_right_reads = masurca_illumina(mp_sample_list, verified_mp_read_folder)

				configfile.write('PE= pe {pe_frag_mean} {pe_frag_sd} {pe_left_reads} {pe_right_reads}\n'.format(pe_frag_mean = self.pe_frag_mean, pe_frag_sd = self.pe_frag_sd, pe_left_reads = pe_left_reads, pe_right_reads = pe_right_reads))
				configfile.write('JUMP= sh {mp_frag_mean} {mp_frag_sd} {mp_left_reads} {mp_right_reads}\n'.format(mp_frag_mean = self.mp_frag_mean, mp_frag_sd = self.mp_frag_sd, mp_left_reads = mp_left_reads, mp_right_reads = mp_right_reads))
			
			if self.seq_platforms=="pe-ont":
				pe_left_reads,pe_right_reads = masurca_illumina(pe_sample_list, verified_pe_read_folder)
				ont_reads = masurca_longread(ont_sample_list, verified_ont_read_folder)

				configfile.write('PE= pe {pe_frag_mean} {pe_frag_sd} {pe_left_reads} {pe_right_reads}\n'.format(pe_frag_mean = self.pe_frag_mean, pe_frag_sd = self.pe_frag_sd, pe_left_reads = pe_left_reads, pe_right_reads = pe_right_reads))
				configfile.write('NANOPORE={ont_reads}\n'.format(ont_reads = ont_reads))
			
			if self.seq_platforms=="pe-pac":
				pe_left_reads,pe_right_reads = masurca_illumina(pe_sample_list, verified_pe_read_folder)
				pac_reads = masurca_longread(pac_sample_list, verified_pac_read_folder)

				configfile.write('PE= pe {pe_frag_mean} {pe_frag_sd}  {pe_left_reads}  {pe_right_reads}\n'.format(pe_frag_mean = self.pe_frag_mean, pe_frag_sd = self.pe_frag_sd, pe_left_reads = pe_left_reads, pe_right_reads = pe_right_reads))
				configfile.write('PACBIO={pac_reads}\n'.format(pac_reads = pac_reads))

			configfile.write('END\n')
			configfile.write('\n')
			configfile.write('PARAMETERS\n')
			configfile.write('EXTEND_JUMP_READS=0\n')
			configfile.write('GRAPH_KMER_SIZE=auto\n')
			configfile.write('EXTEND_JUMP_READS=0\n')
			configfile.write('USE_LINKING_MATES = 0\n')
			configfile.write('USE_GRID=0\n')
			configfile.write('GRID_ENGINE=SGE\n')
			configfile.write('GRID_QUEUE=all.q\n')
			configfile.write('GRID_BATCH_SIZE=500000000\n')
			configfile.write('LHE_COVERAGE=25\n')
			configfile.write('MEGA_READS_ONE_PASS=0\n')
			configfile.write('LIMIT_JUMP_COVERAGE = 300\n')
			configfile.write('CA_PARAMETERS =  cgwErrorRate=0.15\n')
			configfile.write('NUM_THREADS = {threads}\n'.format(threads=GlobalParameter().threads))
			configfile.write('JF_SIZE = {JFSIZE}\n'.format(JFSIZE=JFSIZE))
			configfile.write('FLYE_ASSEMBLY={assembler}\n'.format(assembler=assembler))
			configfile.write('END\n')
			configfile.close()
			
			print("Generated the Masurca Config File at {path}".format(path=masurca_config_path))

			print('')
			print('')

			masurca_config_cmd = "[ -d  {masurca_assembly_folder} ] || mkdir -p {masurca_assembly_folder}; " \
							 "cd {masurca_assembly_folder}; " \
							 "masurca {masurca_config_path}; ".format(masurca_assembly_folder=masurca_assembly_folder,
																	masurca_config_path=masurca_config_path)

			print("****** NOW RUNNING COMMAND ******: " + masurca_config_cmd)
			run_cmd(masurca_config_cmd)


			masurca_run_cmd = "[ -d  {masurca_assembly_folder} ] || mkdir -p {masurca_assembly_folder}; " \
						  "cd {masurca_assembly_folder}; " \
						  "/usr/bin/time -v ./assemble.sh " \
						  .format(masurca_assembly_folder=masurca_assembly_folder)

			print("****** NOW RUNNING COMMAND ******: " + masurca_run_cmd)
			run_cmd(masurca_run_cmd)		

			#BOOST_ROOT=/share/home/zhangyj/biosoft/boost_1_72_0 ./install.sh	