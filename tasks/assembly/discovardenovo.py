#!/usr/bin/env python3
import os
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

def run_cmd(cmd):
		p = subprocess.Popen(cmd, bufsize=-1,
												 shell=True,
												 universal_newlines=True,
												 stdout=subprocess.PIPE,
												 executable='/bin/bash')
		output = p.communicate()[0]
		return output


class discovardenovo(luigi.Task):

	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	seq_platforms = luigi.Parameter(default="pe")
	assembly_name = GlobalParameter().assembly_name  
	pre_process_reads = luigi.ChoiceParameter(choices=["yes","no"],var_type=str)

	def requires(self):

		if self.pre_process_reads=="yes":
			return [cleanFastq(seq_platforms="pe",sampleName=i)
				for i in [line.strip()
					for line in
						open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

		if self.pre_process_reads=="no":
			return [reformat(seq_platforms="pe",sampleName=i)
				for i in [line.strip()
					for line in
						open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

	def output(self):
		dsdn_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "DiscovarDeNovo",self.assembly_name,"a.final"  + "/")
		return {'out': luigi.LocalTarget(dsdn_assembly_folder + "a.lines.fasta")}

	def run(self):
		dsdn_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "DiscovarDeNovo",self.assembly_name + "/")
		dsdn_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"log", "GenomeAssembly", "DiscovarDeNovo"+ "/")
				
		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")

		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")



		def dsdn_illumina(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				pe_cleaned_read_folder = inputDir
				result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
				result1 = [pe_cleaned_read_folder + x + "," + pe_cleaned_read_folder +y for x, y in result]
				parse_string = ','.join(result1)
				return parse_string

		if self.pre_process_reads=="yes":
			cmd_dsdn_pe = dsdn_illumina(pe_sample_list,cleaned_pe_read_folder)
		if self.pre_process_reads=="no":
			cmd_dsdn_pe = dsdn_illumina(pe_sample_list,verified_pe_read_folder)



		run_cm_discovar_pe ="[ -d  {dsdn_assembly_folder} ] || mkdir -p {dsdn_assembly_folder}; " \
							"mkdir -p {dsdn_assembly_log_folder}; " \
							"/usr/bin/time -v DiscovarDeNovo  " \
							"MEMORY_CHECK=True " \
							"MAX_MEM_GB={maxMem} " \
							"NUM_THREADS={threads} " \
							"OUT_DIR={dsdn_assembly_folder} " \
							"READS={cmd_dsdn_pe} " \
							"2>&1 | tee {dsdn_assembly_log_folder}Discovar_DeNovo_assembly.log " \
						.format(dsdn_assembly_folder=dsdn_assembly_folder,
										dsdn_assembly_log_folder=dsdn_assembly_log_folder,
										threads=GlobalParameter().threads,
										maxMem=GlobalParameter().maxMemory,                    
										cmd_dsdn_pe=cmd_dsdn_pe)
				
		print("****** NOW RUNNING COMMAND ******: " + run_cm_discovar_pe)
		run_cmd(run_cm_discovar_pe)


		cmd_rename_contigs ="cp {dsdn_assembly_folder}a.final/a.lines.fasta " \
							"{dsdn_assembly_folder}{assembly_name}_contigs.fasta ".format(dsdn_assembly_folder=dsdn_assembly_folder,assembly_name=self.assembly_name)
		run_cmd(cmd_rename_contigs)