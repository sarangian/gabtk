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
class abyss(luigi.Task):
	threads = GlobalParameter().threads
	maxMemory = GlobalParameter().maxMemory
	projectName = GlobalParameter().projectName
	domain=GlobalParameter().domain
	assembly_name=GlobalParameter().assembly_name
	pe_read_dir=GlobalParameter().pe_read_dir

	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)

	seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end','pe-mp: paired-end and mate-pair',pe-ont: paired-end and nanopore, pe-pac: paired-end and pacbio, ont: nanopore, pac: pacbio]",
											 choices=["pe", "mp","pe-mp", "pe-ont", "pe-pac","ont","pac"], var_type=str)
   

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




		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
			return [
				[cleanFastq(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "ont_samples.lst"))],

				[filtlong(sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
			]

		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
			return [
				[reformat(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[reformat(seq_platforms="ont",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
			]


		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
			return [
				[cleanFastq(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[filtlong(seq_platforms="pac",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
			]

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
			return [
				[reformat(seq_platforms="pe", sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

				[kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))],

				[reformat(seq_platforms="pac",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
			]

		

	def output(self):
		abyss_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "ABYSS" +"/")
		return {'out': luigi.LocalTarget(abyss_assembly_folder + self.assembly_name + "-scaffolds.fa")}

	def run(self):
		abyss_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "ABYSS" +"/")
		abyss_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "ABYSS",self.assembly_name + "/")


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

	  
		pe_lib="lib="
		mp_lib="mp="

		def abyss_illumina(samplefile,libType,inputDir):
			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()
				samples = [x for x in sample_name_list]

				lib_string = libType + "'" + ' '.join(samples) + "'"

				left_read_name_suffix = '_R1.fastq'
				right_read_name_suffix = '_R2.fastq'

				left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
				right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]

				samples = [x for x in sample_name_list]

				all_samples = [sublist for sublist in zip(samples, left_read_name_list, right_read_name_list)]
				all_samples_with_indir_list = [' ' + x + '=' + "'" + inputDir + y + " " + inputDir + z + "'" for x, y, z in all_samples]
				all_samples_with_indir_string = ' '.join(all_samples_with_indir_list)
				lib_string_indir_sample_string = lib_string + all_samples_with_indir_string
				return lib_string_indir_sample_string


		def abyss_longread(samplefile,inputDir):

			with open(samplefile) as fh:
				sample_name_list = fh.read().splitlines()

				samples = [x for x in sample_name_list]

				lib_string = "long=" + "'" + ' '.join(samples) + "'" + " "
				read_name_suffix = '.fastq'
				read_name_list = [x + read_name_suffix for x in sample_name_list]
				long_samples_list = [sublist for sublist in zip(samples, read_name_list)]
				long_samples_with_indir_list = [' ' + x + '=' + "'" + inputDir + y + "'" for x, y in long_samples_list]
				long_samples_with_indir_string = ' '.join(long_samples_with_indir_list)
				lib_string_long_samples_with_indir_string = lib_string + long_samples_with_indir_string
				return lib_string_long_samples_with_indir_string


		if self.pre_process_reads == "yes" and self.seq_platforms=="pe":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, cleaned_pe_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-mp":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, cleaned_pe_read_folder)
			abyss_mp_reads = abyss_illumina(mp_sample_list, mp_lib, cleaned_mp_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-pac":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, cleaned_pe_read_folder)
			abyss_pac_reads = abyss_longread(pac_sample_list, cleaned_pac_read_folder)

		if self.pre_process_reads == "yes" and self.seq_platforms=="pe-ont":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, cleaned_pe_read_folder)
			abyss_ont_reads = abyss_longread(ont_sample_list, cleaned_ont_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, verified_pe_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-mp":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, verified_pe_read_folder)
			abyss_mp_reads = abyss_illumina(mp_sample_list, mp_lib, verified_mp_read_folder)

		if self.pre_process_reads == "no" and self.seq_platforms=="pe-pac":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, verified_pe_read_folder)
			abyss_pac_reads = abyss_longread(pac_sample_list, verified_pac_read_folder)
			
		if self.pre_process_reads == "no" and self.seq_platforms=="pe-ont":
			abyss_pe_reads = abyss_illumina(pe_sample_list, pe_lib, verified_pe_read_folder)
			abyss_ont_reads = abyss_longread(ont_sample_list, verified_ont_read_folder)


		kmer = optimal_kmer((os.path.join(os.getcwd(),self.projectName, "GenomeAssembly", "KmerGenie", "kmergenni_pe.lst")))

		if self.seq_platforms=="pe":

			run_cmd_abyss_pe = "[ -d  {abyss_assembly_folder} ] || mkdir -p {abyss_assembly_folder}; " \
						   "mkdir -p {abyss_assembly_log_folder}; cd {abyss_assembly_folder}; " \
						   "/usr/bin/time -v abyss-pe " \
						   "k={kmer} " \
						   "np={threads} " \
						   "name={assembly_name} " \
						   "{abyss_pe_reads} " \
						   "-o {abyss_assembly_folder} " \
						   "2>&1 | tee {abyss_assembly_log_folder}abyss_assembly.log " \
			.format(abyss_assembly_folder=abyss_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					assembly_name=self.assembly_name,
					abyss_assembly_log_folder=abyss_assembly_log_folder,
					abyss_pe_reads=abyss_pe_reads)

		if self.seq_platforms=="pe-mp":

			run_cmd_abyss_pe_mp = "[ -d  {abyss_assembly_folder} ] || mkdir -p {abyss_assembly_folder}; " \
							  "mkdir -p {abyss_assembly_log_folder}; cd {abyss_assembly_folder}; " \
							  "/usr/bin/time -v abyss-pe " \
							  "k={kmer} " \
							  "np={threads} " \
							  "name={assembly_name} " \
							  "{abyss_pe_reads} {abyss_mp_reads} " \
							  "-o {abyss_assembly_folder} " \
							  "2>&1 | tee {abyss_assembly_log_folder}abyss_assembly.log " \
			.format(abyss_assembly_folder=abyss_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					assembly_name=self.assembly_name,
					abyss_assembly_log_folder=abyss_assembly_log_folder,
					abyss_pe_reads=abyss_pe_reads,
					abyss_mp_reads=abyss_mp_reads)

		
		if self.seq_platforms=="pe-ont":
			run_cmd_abyss_pe_ont = "[ -d  {abyss_assembly_folder} ] || mkdir -p {abyss_assembly_folder}; " \
							  "mkdir -p {abyss_assembly_log_folder}; cd {abyss_assembly_folder}; " \
							  "/usr/bin/time -v abyss-pe " \
							  "k={kmer} " \
							  "np={threads} " \
							  "name={assembly_name} " \
							  "{abyss_pe_reads} {abyss_ont_reads}" \
							  "-o {abyss_assembly_folder} " \
							  "2>&1 | tee {abyss_assembly_log_folder}abyss_assembly.log " \
			.format(abyss_assembly_folder=abyss_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					assembly_name=self.assembly_name,
					abyss_assembly_log_folder=abyss_assembly_log_folder,
					abyss_pe_reads=abyss_pe_reads,
					abyss_ont_reads=abyss_ont_reads)

		if self.seq_platforms=="pe-pac":
			run_cmd_abyss_pe_pac = "[ -d  {abyss_assembly_folder} ] || mkdir -p {abyss_assembly_folder}; " \
							  "mkdir -p {abyss_assembly_log_folder}; cd {abyss_assembly_folder}; " \
							  "/usr/bin/time -v abyss-pe " \
							  "k={kmer} " \
							  "np={threads} " \
							  "name={assembly_name} " \
							  "{abyss_pe_reads} {abyss_pac_reads} " \
							  "-o {abyss_assembly_folder} " \
							  "2>&1 | tee {abyss_assembly_log_folder}abyss_assembly.log " \
			.format(abyss_assembly_folder=abyss_assembly_folder,
					kmer=kmer,
					threads=GlobalParameter().threads,
					assembly_name=self.assembly_name,
					abyss_assembly_log_folder=abyss_assembly_log_folder,
					abyss_pe_reads=abyss_pe_reads,
					abyss_pac_reads=abyss_pac_reads)

	   

		if self.seq_platforms == "pe" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe)
			run_cmd(run_cmd_abyss_pe)

		if self.seq_platforms == "pe" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe)
			run_cmd(run_cmd_abyss_pe)


		if self.seq_platforms == "pe-mp" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_mp)
			run_cmd(run_cmd_abyss_pe_mp)

		if self.seq_platforms == "pe-mp" and self.pre_process_reads == "no":
			
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_mp)
			run_cmd(run_cmd_abyss_pe_mp)


		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_ont)
			run_cmd(run_cmd_abyss_pe_ont)

		if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_ont)
			run_cmd(run_cmd_abyss_pe_ont)



		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
			kmergenie_formater_cleanFastq(pe_sample_list)
			print("Optimal Kmer: ", kmer)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_pac)
			run_cmd(run_cmd_abyss_pe_pac)

	  

		if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
			kmergenie_formater_reformat(pe_sample_list)
			print("Optimal Kmer: ", kmer)


			print('test: ', abyss_pac_reads)
			print("****** NOW RUNNING COMMAND ******: " + run_cmd_abyss_pe_pac)
			run_cmd(run_cmd_abyss_pe_pac)