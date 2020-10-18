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


def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output



#####################################################################################################################################
class ray(luigi.Task):

	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	seq_platforms = luigi.Parameter(default="pe")
	assembly_name = GlobalParameter().assembly_name  
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end','pe-mp: paired-end and mate-pair',pe-ont: paired-end and nanopore, pe-pac: paired-end and pacbio]",
											 choices=["pe", "pe-mp", "pe-ont", "pe-pac"], var_type=str)

	def requires(self):
		if self.seq_platforms == "pe" and self.pre_process_reads =="yes":
			return [[cleanFastq(seq_platforms="pe", sampleName=i)
					 for i in [line.strip()
						for line in
							open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

					[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],
					]

		if self.seq_platforms == "pe" and self.pre_process_reads =="no":
			return [[reformat(seq_platforms="pe",sampleName=i)
				for i in [line.strip()
					for line in
						open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

					[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],
					]

		
		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
			return [
				[cleanFastq(seq_platforms="pe", sampleName=i)
				 for i in [line.strip() for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[cleanFastq(seq_platforms="mp", sampleName=i)
				 for i in [line.strip() for line in
						   open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
			   ]

		if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
			return [
				[reformat(seq_platforms="pe",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[reformat(seq_platforms="mp", sampleName=i) for i in [line.strip() for line in open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
			]
			
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

		ray_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "RAY_" + self.seq_platforms, self.assembly_name +"/")
		return {'out': luigi.LocalTarget(ray_assembly_folder + "Scaffolds.fasta" + "/")}

	def run(self):
		ray_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "RAY_" + self.seq_platforms, self.assembly_name +"/")
		ray_folder = os.path.join(os.getcwd(), self.projectName, "GenomeAssembly", "RAY_" + self.seq_platforms + "/")

		ray_assembly_log_folder = os.path.join(os.getcwd(), self.projectName, "log", "GenomeAssembly", "RAY", self.assembly_name + "/")


		if self.seq_platforms=="pe":
			pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		if self.seq_platforms=="pe-mp":
			pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
			mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
		if self.seq_platforms=="pe-ont":
			pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
			ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		if self.seq_platforms=="pe-pac":
			pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
			pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")


		verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
		verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")
		verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
		verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

		cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		cleaned_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "MP-Reads" + "/")
		cleaned_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
		cleaned_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")


		def ray_illumina(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'
				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
				input_read_folder = inputDir
				result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
				result1 = ["-p " + input_read_folder + x + " " + input_read_folder + y for x, y in result]
				parse_string = ' '.join(result1)
				return parse_string

		def ray_longread(samplefile,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fastq'
				read_name_list = [x + read_name_suffix for x in sample_name_list]
				input_read_folder = inputDir
				reads_list = [input_read_folder + x for x in read_name_list]
				long_reads = ' '.join(reads_list)
				return long_reads

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe":
			cmd_ray_pe = ray_illumina(pe_sample_list,cleaned_pe_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-mp":
			cmd_ray_mp = ray_illumina(mp_sample_list,cleaned_mp_read_folder)
			cmd_ray_pe = ray_illumina(pe_sample_list,cleaned_pe_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-pac":	
			ray_pac_files = ray_longread(pac_sample_list,cleaned_pac_read_folder)
			cmd_ray_pe = ray_illumina(pe_sample_list,cleaned_pe_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-ont":
			cmd_ray_pe = ray_illumina(pe_sample_list,cleaned_pe_read_folder)	
			ray_ont_files = ray_longread(ont_sample_list,cleaned_ont_read_folder)


		if self.pre_process_reads == "no" and self.seq_platforms=="pe":
			cmd_ray_pe = ray_illumina(pe_sample_list,verified_pe_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-mp":
			cmd_ray_mp = ray_illumina(mp_sample_list,verified_mp_read_folder)
			cmd_ray_pe = ray_illumina(pe_sample_list,verified_pe_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-pac":	
			ray_pac_files = ray_longread(pac_sample_list,verified_pac_read_folder)
			cmd_ray_pe = ray_illumina(pe_sample_list,verified_pe_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-ont":
			cmd_ray_pe = ray_illumina(pe_sample_list,verified_pe_read_folder)	
			ray_ont_files = ray_longread(ont_sample_list,verified_ont_read_folder)


		kmergenie_sample_list=os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "KmerGenie", GlobalParameter().assembly_name, GlobalParameter().assembly_name+".lst")


		kmer = optimal_kmer(kmergenie_sample_list)

		if self.seq_platforms=="pe":

			run_cmd_ray_pe = "[ -d  {ray_assembly_log_folder} ] || mkdir -p {ray_assembly_log_folder}; " \
						   "/usr/bin/time -v  mpiexec -n {threads} Ray " \
						   "-k {kmer} " \
						   "{cmd_ray_pe} " \
						   "-o {ray_assembly_folder} " \
						   "2>&1 | tee {ray_assembly_log_folder}ray_assembly.log " \
			.format(ray_assembly_folder=ray_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					ray_assembly_log_folder=ray_assembly_log_folder,
					cmd_ray_pe=cmd_ray_pe)

		if self.seq_platforms=="pe-mp":
			run_cmd_ray_pe_mp = "[ -d  {ray_assembly_log_folder} ] || mkdir -p {ray_assembly_log_folder}; " \
							"/usr/bin/time -v  mpiexec -n {threads} Ray " \
							"-k {kmer} " \
							"{cmd_ray_pe} {cmd_ray_mp} " \
							"-o {ray_assembly_folder} " \
							"2>&1 | tee {ray_assembly_log_folder}ray_assembly.log " \
			.format(ray_assembly_folder=ray_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					ray_assembly_log_folder=ray_assembly_log_folder,
					cmd_ray_pe=cmd_ray_pe,
					cmd_ray_mp=cmd_ray_mp)


		if self.seq_platforms=="pe-pac":
			run_merge_pac_fastq = "[ -d  {ray_folder} ] || mkdir -p {ray_folder}; cd {ray_folder};" \
							  "cat {ray_pac_files} > {assembly_name}_pac.fastq; ". \
								format(ray_folder=ray_folder, ray_pac_files=ray_pac_files,
				   				assembly_name=self.assembly_name)

			run_cmd_ray_pac_fq2fa = "[ -d  {ray_folder} ] || mkdir -p {ray_folder}; cd {ray_folder};" \
								" fq2fa {ray_folder}{assembly_name}_pac.fastq {assembly_name}_pac.fas ". \
								format(ray_folder=ray_folder, assembly_name=self.assembly_name)
			run_cmd_ray_pe_pac = "[ -d  {ray_assembly_log_folder} ] || mkdir -p {ray_assembly_log_folder}; " \
							 "/usr/bin/time -v  mpiexec -n {threads} Ray " \
							 "-k {kmer} " \
							 "{cmd_ray_pe} -l {ray_folder}{assembly_name}_pac.fas " \
							 "-o {ray_assembly_folder} " \
							 "2>&1 | tee {ray_assembly_log_folder}ray_assembly.log " \
			.format(ray_assembly_folder=ray_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					ray_assembly_log_folder=ray_assembly_log_folder,
					ray_folder=ray_folder,
					cmd_ray_pe=cmd_ray_pe,
					assembly_name=self.assembly_name)

		if self.seq_platforms=="pe-ont":
			run_merge_ont_fastq = "[ -d  {ray_folder} ] || mkdir -p {ray_folder}; cd {ray_folder};" \
							  "cat {ray_ont_files} > {assembly_name}_ont.fastq; ". \
								format(ray_folder=ray_folder, ray_ont_files=ray_ont_files,
				   				assembly_name=self.assembly_name)

			run_cmd_ray_ont_fq2fa = "[ -d  {ray_folder} ] || mkdir -p {ray_folder}; cd {ray_folder};" \
								" fq2fa {ray_folder}{assembly_name}_ont.fastq {assembly_name}_ont.fas ". \
								format(ray_folder=ray_folder, assembly_name=self.assembly_name)
			run_cmd_ray_pe_ont = "[ -d  {ray_assembly_log_folder} ] || mkdir -p {ray_assembly_log_folder}; " \
							 "/usr/bin/time -v  mpiexec -n {threads} Ray " \
							 "-k {kmer} " \
							 "{cmd_ray_pe} -l {assembly_name}_ont.fas " \
							 "-o {ray_assembly_folder} " \
							 "2>&1 | tee {ray_assembly_log_folder}ray_assembly.log " \
			.format(ray_assembly_folder=ray_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					ray_assembly_log_folder=ray_assembly_log_folder,
					cmd_ray_pe=cmd_ray_pe,
					assembly_name=self.assembly_name)


		if self.seq_platforms == "pe" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe)
			run_cmd(run_cmd_ray_pe)

		if self.seq_platforms == "pe" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe)
			run_cmd(run_cmd_ray_pe)

		if self.seq_platforms == "pe-mp" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_mp)
			run_cmd(run_cmd_ray_pe_mp)

		if self.seq_platforms == "pe-mp" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_mp)
			run_cmd(run_cmd_ray_pe_mp)

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)

			print("****** NOW RUNNING COMMAND ******: " + run_merge_pac_fastq)
			run_cmd(run_merge_pac_fastq)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pac_fq2fa)
			run_cmd(run_cmd_ray_pac_fq2fa)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_pac)
			run_cmd(run_cmd_ray_pe_pac)

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)

			print("****** NOW RUNNING COMMAND ******: " + run_merge_pac_fastq)
			run_cmd(run_merge_pac_fastq)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pac_fq2fa)
			run_cmd(run_cmd_ray_pac_fq2fa)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_pac)
			run_cmd(run_cmd_ray_pe_pac)

		#PE ONT
		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_merge_ont_fastq)
			run_cmd(run_merge_ont_fastq)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_ont_fq2fa)
			run_cmd(run_cmd_ray_ont_fq2fa)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_ont)
			run_cmd(run_cmd_ray_pe_ont)

		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_merge_ont_fastq)
			run_cmd(run_merge_pac_fastq)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_ont_fq2fa)
			run_cmd(run_cmd_ray_ont_fq2fa)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_ray_pe_ont)
			run_cmd(run_cmd_ray_pe_ont)