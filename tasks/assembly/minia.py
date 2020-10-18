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

'''

minia=os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly","MINIA")

createFolder(minia)

def minia_kmer(readlist):
	kmergenie_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly","MINIA" + "/")

	command = "[ -d  {kmergenie_folder} ] || mkdir -p {kmergenie_folder}; " \
				  "cd {kmergenie_folder}; " \
				  "kmergenie {kmergenie_folder}minia.fofn " \
				  .format(kmergenie_folder=kmergenie_folder)

	print("Estimating Optimal Kmers using kmergenie")
	print("-----------------------------------------")
	print("Command: ", command)
	print("\n")

	proc = subprocess.Popen(command,
							shell=True,
							stdin=subprocess.PIPE,
							stdout=subprocess.PIPE,
							stderr=subprocess.PIPE)

	stdout_value, stderr_value = proc.communicate()

	parse_output = stdout_value.strip().decode("utf-8")

	p = re.compile(r'^best k:\s+(\d{2})', re.M)
	optimal_k = ' '.join(re.findall(p, parse_output))

	return optimal_k

'''

class minia(luigi.Task):
	projectName = GlobalParameter().projectName
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	assembly_name=GlobalParameter().assembly_name

	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end',pe-mp: paired-end and mate-pair']",choices=["pe","pe-mp"], var_type=str)


	def requires(self):
		if self.seq_platforms == "pe" and self.pre_process_reads=="yes":
			return [
					[cleanFastq(seq_platforms="pe",sampleName=i) 
						for i in [line.strip() 
							for line in open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))
								 ]
					 ],

					[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]

					]


		if self.seq_platforms == "pe" and self.pre_process_reads=="no":
			return [
						[reformat(seq_platforms="pe",sampleName=i) 
							for i in [line.strip() 
								for line in open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]
						],
						
						[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]
					]

		# Paired-end with Mate-pair
		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
			return [
						[cleanFastq(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


						[cleanFastq(seq_platforms="mp",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]],

						[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


					]

		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
			return [
						[reformat(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[reformat(seq_platforms="mp",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]],

						[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]
					]




	def output(self):
		minia_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MINIA" , self.assembly_name + "/")
		return {'out': luigi.LocalTarget(minia_assembly_folder + "minia.contigs.fa")}

	def run(self):
		minia_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MINIA", self.assembly_name + "/")
		
		minia_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "log", "GenomeAssembly", "MINIA" +  "/")

		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
		ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")



		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")
		verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
		verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		cleaned_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "MP-Reads" + "/")
		cleaned_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
		cleaned_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")



		def minia_illumina(samplefile,inputDir):
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

		def minia_longread(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fastq'
				read_name_list = [x + read_name_suffix for x in sample_name_list]
				input_read_folder = inputDir
				reads_list = [input_read_folder + x for x in read_name_list]
				long_reads = ' '.join(reads_list)
				return long_reads

	  
		
		if self.pre_process_reads=="yes" and self.seq_platforms=="pe":
			pe_left_reads,pe_right_reads= minia_illumina(pe_sample_list,cleaned_pe_read_folder)

			reads = pe_left_reads+"\n"+ pe_right_reads
			read_list = '\n'.join(reads)
			with open((os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "MINIA","minia.fofn")), 'w') as fh:
				fh.writelines("%s"  % read for read in read_list)


		if self.pre_process_reads=="yes" and self.seq_platforms=="pe-mp":
			pe_left_reads,pe_right_reads= minia_illumina(pe_sample_list,cleaned_pe_read_folder)
			mp_left_reads,mp_right_reads= minia_illumina(mp_sample_list,cleaned_mp_read_folder)

			reads = pe_left_reads + pe_right_reads + mp_left_reads + mp_right_reads
			read_list = '\n'.join(reads)

			with open((os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "MINIA","minia.fofn")), 'w') as fh:
				fh.writelines("%s"  % read for read in read_list)


		if self.pre_process_reads=="no" and self.seq_platforms=="pe":
			pe_left_reads,pe_right_reads= minia_illumina(pe_sample_list,verified_pe_read_folder)

			reads = pe_left_reads+"\n"+ pe_right_reads
			read_list = '\n'.join(reads)
			with open((os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "MINIA","minia.fofn")), 'w') as fh:
				fh.writelines("%s"  % read for read in read_list)


		if self.pre_process_reads=="no" and self.seq_platforms=="pe-mp":
			pe_left_reads,pe_right_reads= minia_illumina(pe_sample_list,verified_pe_read_folder)
			mp_left_reads,mp_right_reads= minia_illumina(mp_sample_list,verified_mp_read_folder)

			reads = pe_left_reads +"\n"+ pe_right_reads +"\n"+ mp_left_reads +"\n"+ mp_right_reads
			#read_list = ''.join(reads)

			with open((os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "MINIA","minia.fofn")), 'w') as fh:
				fh.writelines("%s"  % read for read in reads)

		

		########################################################################
		kmergenie_sample_list=os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "KmerGenie", GlobalParameter().assembly_name, GlobalParameter().assembly_name+".lst")
		kmer = optimal_kmer(kmergenie_sample_list)
		print("Optimal Kmer: ", kmer)
		

		run_cmd_minia = "[ -d  {minia_assembly_folder} ] || mkdir -p {minia_assembly_folder}; " \
						"mkdir -p {minia_assembly_log_folder}; cd {minia_assembly_folder}; " \
						"/usr/bin/time -v minia " \
						"-kmer-size {kmer} " \
						"-nb-cores {threads} " \
						"-in {kmergenie_sample_list} " \
						"2>&1 | tee {minia_assembly_log_folder}minia_assembly.log " \
			.format(minia_assembly_folder=minia_assembly_folder,
					kmer=kmer,
					kmergenie_sample_list=kmergenie_sample_list,
					threads=GlobalParameter().threads,
					minia_assembly_log_folder=minia_assembly_log_folder)

		if self.seq_platforms=="pe" or self.seq_platforms=="pe-mp":

			print("****** NOW RUNNING COMMAND ******: " + run_cmd_minia)
			print(run_cmd(run_cmd_minia))




