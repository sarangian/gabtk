import luigi
import os
import subprocess
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.reFormatReads import reformat
from tasks.readCleaning.reFormatReads import reformatReads


class GlobalParameter(luigi.Config):
	assembly_name=luigi.Parameter()
	projectName=luigi.Parameter()
	pac_read_dir=luigi.Parameter()
	pac_read_suffix=luigi.Parameter()
	genome_size=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()

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


class mecat2(luigi.Task):
	projectName = GlobalParameter().projectName
	seq_platform = luigi.Parameter(default="pac")
	genome_size = GlobalParameter().genome_size
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	pre_process_reads = luigi.ChoiceParameter(choices=["yes","no"],var_type=str)


	
	def requires(self):
		if self.pre_process_reads=="yes":
			return [filtlong(seq_platforms="pac",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]


		if self.pre_process_reads=="no":
			return [reformat(seq_platforms="pac",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]


	def output(self):

		mecat_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MECAT2_" + self.seq_platform +"/")
		return {'out': luigi.LocalTarget(mecat_assembly_folder,"4-fsa","contigs.fasta")}


		mecat2_correct_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CorrectReads","PAC-Reads" + "/")
		return {'out': luigi.LocalTarget(mecat2_correct_folder,"cns_final.fasta")}

	def run(self):
		mecat2_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MECAT2_" + self.seq_platform +"/")
		mecat2_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "MECAT2", self.assembly_name+ "/")


		if self.pre_process_reads=="no":
			lr_cleaned_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","PAC-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			lr_cleaned_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","PAC-Reads" + "/")

		long_read_list=os.path.join(lr_cleaned_read_folder, "long_read.lst")
		
		config_folder_path=os.path.join(os.getcwd(), "configuration")
		createFolder(config_folder_path)
		
		mecat_assembly_config_path=os.path.join(os.getcwd(), "configuration", "mecat2_assembly.config")
		
		assembly_name=self.assembly_name
		#clean_read_list= os.path.join(os.getcwd(), "CleanedReads", "Cleaned_Long_Reads", "clean_read.txt")
		genome_size=self.genome_size
		threads=GlobalParameter().threads

		def mecat2_config_generator(lrfile):
			if self.pre_process_reads=="no":
				long_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","PAC-Reads" + "/")
			
			if self.pre_process_reads=="yes":
				long_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","PAC-Reads" + "/")

				createFolder(long_read_folder)

				long_read_list= os.path.join(long_read_folder, "long_read.lst")


				with open(lrfile) as fh:
					with open(long_read_list, "w") as fo:
						sample_name_list = fh.read().splitlines()
						read_name_suffix = '.fastq'
						read_name_list = [lr_cleaned_read_folder + x + read_name_suffix for x in sample_name_list]
						lr_parse_string = ' '.join(read_name_list)
						fo.write(lr_parse_string)


		def fq2fa_input(lrfile):
			if self.pre_process_reads=="no":
				long_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","PAC-Reads" + "/")
			
			if self.pre_process_reads=="yes":
				long_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","PAC-Reads" + "/")

			long_read_list= os.path.join(long_read_folder, "long_read.lst")
			createFolder(long_read_folder)

			with open(lrfile) as fh:
				with open(long_read_list, "w") as fo:
					sample_name_list = fh.read().splitlines()
					read_name_suffix = '.fastq'
					read_name_list = [lr_cleaned_read_folder + x + read_name_suffix for x in sample_name_list]
					input_string = ' '.join(read_name_list)
					return input_string


		lr_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")
		lr_fasta=mecat2_config_generator(lr_sample_list)
		lr_fastq=fq2fa_input(lr_sample_list)

		fastq2fasta= "cd {lr_cleaned_read_folder}; " \
					 "reformat.sh in={lr_fastq} out={assembly_name}.fasta " \
		.format(lr_cleaned_read_folder=lr_cleaned_read_folder,lr_fastq=lr_fastq,
					assembly_name=self.assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + fastq2fasta)
		run_cmd(fastq2fasta)

		
		with open (mecat_assembly_config_path,'w') as configfile:
			configfile.write('PROJECT={assembly_name}\n'.format(assembly_name=self.assembly_name))
			configfile.write('RAWREADS={lr_cleaned_read_folder}{assembly_name}.fasta\n'.format(lr_cleaned_read_folder=lr_cleaned_read_folder,assembly_name=assembly_name))
			configfile.write('GENOME_SIZE={genome_size}\n'.format(genome_size=genome_size))
			configfile.write('THREADS={threads}\n'.format(threads=GlobalParameter().threads))
			configfile.write('MIN_READ_LENGTH=2000/\n')
			configfile.write('CNS_OVLP_OPTIONS"-kmer_size 13"\n')
			configfile.write('CNS_PCAN_OPTIONS="-p 100000 -k 100"\n')
			configfile.write('CNS_OPTIONS=""\n')
			configfile.write('CNS_OUTPUT_COVERAGE=20\n')
			configfile.write('TRIM_OVLP_OPTIONS="-skip_overhang"\n')
			configfile.write('TRIM_PM4_OPTIONS="-p 100000 -k 100"\n')
			configfile.write('TRIM_LCR_OPTIONS=""\n')
			configfile.write('TRIM_SR_OPTIONS=""\n')
			configfile.write('ASM_OVLP_OPTIONS=""\n')
			configfile.write('FSA_OL_FILTER_OPTIONS="--max_overhang=-1 --min_identity=-1"\n')
			configfile.write('FSA_OL_FILTER_OPTIONS="--max_overhang=-1 --min_identity=-1 --coverage=20"\n')
			configfile.write('FSA_ASSEMBLE_OPTIONS=""\n')
			configfile.write('CLEANUP=0\n')
			


		mecat_cmd = "[ -d  {mecat2_assembly_folder} ] || mkdir -p {mecat2_assembly_folder}; " \
						"mkdir -p {mecat2_assembly_log_folder}; cd {mecat2_assembly_folder}; " \
						"/usr/bin/time -v mecat.pl assemble {mecat_assembly_config_path} " \
						"2>&1 | tee {mecat2_assembly_log_folder}mecat_assembly.log " \
			.format(mecat2_assembly_folder=mecat2_assembly_folder,mecat_assembly_config_path=mecat_assembly_config_path,
					mecat2_assembly_log_folder=mecat2_assembly_log_folder)

		print("****** NOW RUNNING COMMAND ******: " + mecat_cmd)
		run_cmd(mecat_cmd)
