#!/usr/bin/env python3
import os
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
	genome_size=luigi.Parameter()


def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output



#####################################################################################################################################
class haslr(luigi.Task):

	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	seq_platforms = luigi.Parameter(default="pe")
	assembly_name = GlobalParameter().assembly_name  
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe-ont: paired-end and nanopore, pe-pac: paired-end and pacbio]",
											 choices=["pe-ont", "pe-pac"], var_type=str)

	def requires(self):
		
			
		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
			return [
						[cleanFastq(seq_platforms="pe", sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

						[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

						[filtlong(seq_platforms="pac", sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
					]
					
		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
			return [
						[reformat(seq_platforms="pe", sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],
						[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

						[reformat(seq_platforms="pac", sampleName=i)
							for i in [line.strip()for line in
								open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
					]
		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
			return [
						[cleanFastq(seq_platforms="pe", sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],
						[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

						[filtlong(platform="pac",sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
					]
					
		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
			return [
						[reformat(seq_platforms="pe", sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],
						[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

						[reformat(seq_platforms="ont",sampleName=i)
							for i in [line.strip()for line in
								open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
					]

	def output(self):

		haslr_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "HASLR_" + self.seq_platforms, self.assembly_name +"/")
		return {'out': luigi.LocalTarget(haslr_assembly_folder +  self.assembly_name +"_contigs.fasta" + "/")}

	def run(self):
		haslr_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "HASLR_" + self.seq_platforms, self.assembly_name +"/")
		haslr_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "HASLR_" + self.seq_platforms+"/")
		
		haslr_assembly_log_folder = os.path.join(os.getcwd(), self.projectName, "log", "GenomeAssembly", "HASLR", self.assembly_name + "/")

		pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")


		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
		verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		cleaned_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
		cleaned_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")


		def haslr_illumina(samplefile,inDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				pe_cleaned_read_folder = inDir
				result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
				result1 = [pe_cleaned_read_folder + x + " " + pe_cleaned_read_folder + y + " " for x, y in result]
				parse_string = ' '.join(result1)
				return parse_string

		def haslr_longread(samplefile,inDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fastq'
				lr_cleaned_read_folder = inDir
				lr_result = [lr_cleaned_read_folder + x + read_name_suffix for x in sample_name_list]
				lr_parse_string =  ' '.join(lr_result)
				return lr_parse_string


		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-ont":
			haslr_pe_reads = haslr_illumina(pe_sample_list, cleaned_pe_read_folder)
			haslr_ont_reads = haslr_longread(ont_sample_list, cleaned_ont_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-ont":
			haslr_pe_reads = haslr_illumina(pe_sample_list, verified_pe_read_folder)
			haslr_ont_reads = haslr_longread(ont_sample_list, verified_ont_read_folder)
		
		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-pac":
			haslr_pe_reads = haslr_illumina(pe_sample_list,cleaned_pe_read_folder)
			haslr_pac_reads = haslr_longread(pac_sample_list,cleaned_pac_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-pac":
			haslr_pe_reads = haslr_illumina(pe_sample_list,verified_pe_read_folder)
			haslr_pac_reads = haslr_longread(pac_sample_list,verified_pac_read_folder)


		kmergenie_sample_list=os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "KmerGenie", GlobalParameter().assembly_name, GlobalParameter().assembly_name+".lst")

		if self.seq_platforms=="pe-pac":
			long_read_type="pacbio"

		if self.seq_platforms=="pe-ont":
			long_read_type="nanopore"


		kmer = optimal_kmer(kmergenie_sample_list)

		

		if self.seq_platforms == "pe-pac":
			run_merge_pac_fastq = "[ -d  {haslr_folder} ] || mkdir -p {haslr_folder}; cd {haslr_folder};" \
							  "cat {haslr_pac_reads} > {assembly_name}_{long_read_type}.fastq; ". \
								format(haslr_folder=haslr_folder, 
								haslr_pac_reads=haslr_pac_reads,
								long_read_type=long_read_type,
								assembly_name=self.assembly_name)

		if self.seq_platforms == "pe-ont":
			run_merge_ont_fastq = "[ -d  {haslr_folder} ] || mkdir -p {haslr_folder}; cd {haslr_folder};" \
							  "cat {haslr_ont_reads} > {assembly_name}_{long_read_type}.fastq; ". \
								format(haslr_folder=haslr_folder,
									   haslr_ont_reads=haslr_ont_reads,
									   long_read_type=long_read_type,
									   assembly_name=self.assembly_name)

		
		run_cmd_haslr = "[ -d  {haslr_assembly_log_folder} ] || mkdir -p {haslr_assembly_log_folder}; " \
							 "/usr/bin/time -v haslr.py -t {threads} " \
							 "--minia-kmer {kmer} " \
							 "-g {genome_size} " \
							 "-l {haslr_folder}{assembly_name}_{long_read_type}.fastq " \
							 "-x {long_read_type} " \
							 "-s {short_reads} " \
							 "-o {haslr_assembly_folder} " \
							 "2>&1 | tee {haslr_assembly_log_folder}haslr_assembly.log " \
			.format(haslr_assembly_folder=haslr_assembly_folder,
					kmer=kmer, 
					genome_size=GlobalParameter().genome_size,
					threads=GlobalParameter().threads,
					assembly_name=GlobalParameter().assembly_name,
					long_read_type=long_read_type,
					short_reads=haslr_pe_reads,	
					haslr_folder=haslr_folder,				
					haslr_assembly_log_folder=haslr_assembly_log_folder)
		

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)

			print("****** NOW RUNNING COMMAND ******: " + run_merge_pac_fastq)
			run_cmd(run_merge_pac_fastq)
			
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_haslr)
			run_cmd(run_cmd_haslr)

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)

			print("****** NOW RUNNING COMMAND ******: " + run_merge_pac_fastq)
			run_cmd(run_merge_pac_fastq)
			
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_haslr)
			run_cmd(run_cmd_haslr)

		#PE ONT
		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			
			print("****** NOW RUNNING COMMAND ******: " + run_merge_ont_fastq)
			run_cmd(run_merge_ont_fastq)
			
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_ont)
			run_cmd(run_cmd_ray_pe_ont)

		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)

			print("****** NOW RUNNING COMMAND ******: " + run_merge_ont_fastq)
			run_cmd(run_merge_ont_fastq)
			
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_haslr)
			run_cmd(run_cmd_haslr)


		cmd_rename_assembly="cp {haslr_assembly_folder}asm_contigs_k{kmer}_a3_lr25x_b500_s3_sim0.85/asm.final.fa " \
							"{haslr_assembly_folder}{assembly_name}_contigs.fasta ".format(haslr_assembly_folder=haslr_assembly_folder,
								kmer=kmer,
								assembly_name=GlobalParameter().assembly_name)