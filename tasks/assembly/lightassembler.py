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
	projectName=luigi.Parameter()
	genome_size=luigi.Parameter()

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output


class lightAssembler(luigi.Task):
	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	seq_platforms = luigi.Parameter(default="pe")
	assembly_name = GlobalParameter().assembly_name  
	pre_process_reads = luigi.ChoiceParameter(choices=["yes","no"],var_type=str)

	kmer=luigi.IntParameter(default=31,description="Minimal Kmer Length for assembly. [--kmer 31]")
	genome_size=GlobalParameter().genome_size

	def requires(self):

		if self.seq_platforms == "pe" and self.pre_process_reads=="yes":
			return [cleanFastq(seq_platforms="pe",sampleName=i) 
						for i in [line.strip() for line in 
							open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]


		if self.seq_platforms == "pe" and self.pre_process_reads=="no":
			return [reformat(seq_platforms="pe",sampleName=i)
						for i in [line.strip() for line in 
							open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

	def output(self):

		light_assembly_folder = os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "LightAssembler_"+ self.seq_platforms, self.assembly_name + "/")
		return {'out': luigi.LocalTarget(light_assembly_folder + self.assembly_name +"_contigs.fasta")}

	def run(self):
		light_assembly_folder = os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "LightAssembler_"+ self.seq_platforms, self.assembly_name + "/")
		light_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"log", "GenomeAssembly", "LightAssembler",self.assembly_name + "/")


		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		
		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")

		def light_illumina(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				cleaned_read_folder = inputDir
				result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
				result1 = [cleaned_read_folder + x + " " + cleaned_read_folder +y for x, y in result]
				parse_string = ' '.join(result1)
				return parse_string


		if self.pre_process_reads=="yes":
			cmd_light_pe = light_illumina(pe_sample_list,cleaned_pe_read_folder)
		if self.pre_process_reads=="no":
			cmd_light_pe = light_illumina(pe_sample_list,verified_pe_read_folder)

		run_cmd_light_pe = "[ -d  {light_assembly_folder} ] || mkdir -p {light_assembly_folder}; " \
						   "mkdir -p {light_assembly_log_folder}; cd {light_assembly_folder}; " \
						   "/usr/bin/time -v LightAssembler " \
						   "-k {kmer} " \
						   "-G {genome_size} " \
						   "-t {threads} " \
						   "-o {assembly_name}_contigs.fasta " \
						   "{cmd_light_pe} " \
						   "2>&1 | tee {light_assembly_log_folder}light_assembly.log " \
			.format(light_assembly_log_folder=light_assembly_log_folder,
					threads=GlobalParameter().threads,
					kmer=self.kmer,
					genome_size=GlobalParameter().genome_size,
					assembly_name=self.assembly_name,
					light_assembly_folder=light_assembly_folder,
					cmd_light_pe=cmd_light_pe)
		
		print("****** NOW RUNNING COMMAND ******: " + run_cmd_light_pe)
		run_cmd(run_cmd_light_pe)