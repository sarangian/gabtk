#!/usr/bin/env python3
import os
#from tasks.assembly.kmergenie import kmergenie_formater_reformat
#from tasks.assembly.kmergenie import kmergenie_formater_cleanFastq
#from tasks.assembly.kmergenie import optimal_kmer
from tasks.readCleaning.preProcessReads import cleanFastq
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
	genome_size=luigi.Parameter()


def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, executable='/bin/bash')
	output = p.communicate()[0]
	return output


class sparseAssembler(luigi.Task):
	genome_size=GlobalParameter().genome_size
	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	
	assembly_name = GlobalParameter().assembly_name  
	pre_process_reads = luigi.ChoiceParameter(choices=["yes","no"],var_type=str)
	kmer=luigi.IntParameter(default=31,description="Minimal Kmer Length for assembly. [--kmer 31]")
	
	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end',pe-mp: paired-end and mate-pair']",choices=["pe","pe-mp"], var_type=str)



	def requires(self):

		if self.seq_platforms == "pe" and self.pre_process_reads=="yes":
			return [cleanFastq(seq_platforms="pe",sampleName=i) 
						for i in [line.strip() for line in 
							open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]


		if self.seq_platforms == "pe" and self.pre_process_reads=="no":
			return [reformat(seq_platforms="pe",sampleName=i)
						for i in [line.strip() for line in 
							open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

		# Paired-end with Mate-pair
		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
			return [
						[cleanFastq(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[cleanFastq(seq_platforms="mp",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
					]

		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
			return [
						[reformat(seq_platforms="pe",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[reformat(seq_platforms="mp",sampleName=i)
							for i in [line.strip()  for line in
								open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
					]

	def output(self):
		sparse_assembly_folder = os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "SparseAssemble_" + self.seq_platforms, self.assembly_name + "/")
		return {'out': luigi.LocalTarget(sparse_assembly_folder + self.assembly_name+"_contigs.fasta")}

	def run(self):
		sparse_assembly_folder = os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "SparseAssemble_" + self.seq_platforms, self.assembly_name + "/")
		sparse_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "SparseAssembler",self.assembly_name + "/")
		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")

		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
		
		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")
		
		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		cleaned_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "MP-Reads" + "/")
		
		paired_end="i"
		mate_paired="o"

		def sparse_illumina(samplefile,inputDir,libType):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				input_read_folder = inputDir
				result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
				result1 = [ libType+"1" +" "+ input_read_folder + x + " " + libType+"2"+" "+ input_read_folder +y for x, y in result]
				parse_string = ' '.join(result1)
				return parse_string


		if self.pre_process_reads=="yes" and self.seq_platforms=="pe":
			sparse_pe_input= sparse_illumina(pe_sample_list, cleaned_pe_read_folder, paired_end)

		if self.pre_process_reads=="yes" and self.seq_platforms=="pe-mp":
			sparse_pe_input= sparse_illumina(pe_sample_list, cleaned_pe_read_folder, paired_end)
			sparse_mp_input= sparse_illumina(mp_sample_list, cleaned_pe_read_folder, mate_paired)

		if self.pre_process_reads=="no" and self.seq_platforms=="pe":
			sparse_pe_input= sparse_illumina(pe_sample_list, verified_pe_read_folder, paired_end)

		if self.pre_process_reads=="no" and self.seq_platforms=="pe-mp":
			sparse_pe_input= sparse_illumina(pe_sample_list, verified_pe_read_folder, paired_end)
			sparse_mp_input= sparse_illumina(mp_sample_list, verified_pe_read_folder, mate_paired)    	
		

		if self.seq_platforms=="pe":
			run_cmds_parse_pe = "[ -d  {sparse_assembly_folder} ] || mkdir -p {sparse_assembly_folder}; " \
								"mkdir -p {sparse_assembly_log_folder}; cd {sparse_assembly_folder}; " \
								"/usr/bin/time -v SparseAssembler " \
								"k {kmer} " \
								"g 15 BC 1 " \
								"GS {genome_size} " \
								"{sparse_pe_input} " \
								"2>&1 | tee {sparse_assembly_log_folder}sparce_assembly.log ".format(sparse_assembly_log_folder=sparse_assembly_log_folder,
								threads=GlobalParameter().threads,
								kmer=self.kmer,
								genome_size=GlobalParameter().genome_size,
								sparse_assembly_folder=sparse_assembly_folder,
								sparse_pe_input=sparse_pe_input)

			cmd_rename_contigs = "cd {sparse_assembly_folder}; " \
								 "mv Contigs.txt  {assembly_name}_contigs.fasta ".format(sparse_assembly_folder=sparse_assembly_folder,assembly_name=self.assembly_name)
			

			print("****** NOW RUNNING COMMAND ******: " + run_cmds_parse_pe)
			run_cmd(run_cmds_parse_pe)
			run_cmd(cmd_rename_contigs)

		if self.seq_platforms=="pe-mp":	
			run_cmds_parse_pe_mp = "[ -d  {sparse_assembly_folder} ] || mkdir -p {sparse_assembly_folder}; " \
								"mkdir -p {sparse_assembly_log_folder}; cd {sparse_assembly_folder}; " \
								"/usr/bin/time -v SparseAssembler " \
								"k {kmer} " \
								"g 15 BC 1 " \
								"GS {genome_size} " \
								"{sparse_pe_input} {sparse_mp_input} " \
								"2>&1 | tee {sparse_assembly_log_folder}sparce_assembly.log ".format(sparse_assembly_log_folder=sparse_assembly_log_folder,
								threads=GlobalParameter().threads,
								kmer=self.kmer,
								genome_size=self.genome_size,
								sparse_assembly_folder=sparse_assembly_folder,
								sparse_pe_input=sparse_pe_input,
								sparse_mp_input=sparse_mp_input)
			cmd_rename_contigs = "cd {sparse_assembly_folder}; " \
								 "mv Contigs.txt  {assembly_name}_contigs.fasta ".format(sparse_assembly_folder=sparse_assembly_folder,assembly_name=self.assembly_name)
								
			print("****** NOW RUNNING COMMAND ******: " + run_cmds_parse_pe_mp)
			run_cmd(run_cmds_parse_pe_mp)
			run_cmd(cmd_rename_contigs)		