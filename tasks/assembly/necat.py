import luigi
import os
import subprocess
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.reFormatReads import reformat

class GlobalParameter(luigi.Config):
	genome_size=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	assembly_name = luigi.Parameter()

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



class necat(luigi.Task):
	projectName = GlobalParameter().projectName
	seq_platforms = luigi.Parameter(default="ont")
	genome_size = GlobalParameter().genome_size
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	pre_process_reads = luigi.ChoiceParameter(default="no",choices=["yes","no"],var_type=str)


	def requires(self):
		if self.pre_process_reads=="yes":
			return [filtlong(seq_platforms="ont",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]


		if self.pre_process_reads=="no":
			return [reformat(seq_platforms="ont",sampleName=i)
				 for i in [line.strip()
						   for line in
						   open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]


	def output(self):
		necat_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "NECAT_" + self.seq_platforms + "/")
		return {'out': luigi.LocalTarget(necat_assembly_folder,"6-bridge_contigs","bridged_contigs.fasta")}

	def run(self):
		necat_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "NECAT_" + self.seq_platforms + "/")
		necat_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "necat_" + self.seq_platforms +  "/")

		if self.pre_process_reads=="no":
			long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","ONT-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","ONT-Reads" + "/")


				
		def necat_config_generator(lrfile):
			if self.pre_process_reads=="no":
				long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","ONT-Reads" + "/")
			
			if self.pre_process_reads=="yes":
				long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","ONT-Reads" + "/")


			createFolder(long_clean_read_folder)						

			long_read_list= os.path.join(long_clean_read_folder, "long_read.lst")

			with open(lrfile) as fh:
				
				with open(long_read_list, "w") as fo:
					sample_name_list = fh.read().splitlines()
					read_name_suffix = '.fastq'
					read_name_list = [long_clean_read_folder + x + read_name_suffix for x in sample_name_list]
					lr_parse_string = ' '.join(read_name_list)
					fo.write(lr_parse_string)
		
		assembly_name=self.assembly_name
		lr_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		necat_config_generator(lr_sample_list)

		genome_size=self.genome_size
		threads=GlobalParameter().threads
		config_folder_path=os.path.join(os.getcwd(), "configuration")
		necat_assemble_config_path=os.path.join(os.getcwd(), "configuration", "necat_assemble.config")
		createFolder(config_folder_path)

		if self.pre_process_reads=="no":
			long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","ONT-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			long_clean_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","ONT-Reads" + "/")
		

		long_read_list= os.path.join(long_clean_read_folder, "long_read.lst")

		with open (necat_assemble_config_path,'w') as configfile:
			configfile.write('PROJECT={assembly_name}\n'.format(assembly_name=self.assembly_name))
			#configfile.write('ONT_READ_LIST={long_clean_read_folder}long_read.lst\n'.format(long_clean_read_folder=long_clean_read_folder))
			configfile.write('ONT_READ_LIST={long_read_list}\n'.format(long_read_list=long_read_list))
			configfile.write('GENOME_SIZE={genome_size}\n'.format(genome_size=genome_size))
			configfile.write('THREADS={threads}\n'.format(threads=GlobalParameter().threads))
			configfile.write('MIN_READ_LENGTH=3000/\n')
			configfile.write('OVLP_FAST_OPTIONS="-n 500 -z 20 -b 2000 -e 0.5 -j 0 -u 1 -a 1000"\n')
			configfile.write('OVLP_SENSITIVE_OPTIONS="-n 500 -z 10 -e 0.5 -j 0 -u 1 -a 1000"\n')
			configfile.write('CNS_FAST_OPTIONSC"-a 2000 -x 4 -y 12 -l 1000 -e 0.5 -p 0.8 -u 0"\n')
			configfile.write('CNS_SENSITIVE_OPTIONS="-a 2000 -x 4 -y 12 -l 1000 -e 0.5 -p 0.8 -u 0"\n')
			configfile.write('TRIM_OVLP_OPTIONS="-n 100 -z 10 -b 2000 -e 0.5 -j 1 -u 1 -a 400"\n')
			configfile.write('ASM_OVLP_OPTIONS="-n 100 -z 10 -b 2000 -e 0.5 -j 1 -u 0 -a 400"\n')
			configfile.write('NUM_ITER=2\n')
			configfile.write('CNS_OUTPUT_COVERAGE=45\n')
			configfile.write('CLEANUP=0\n')
			configfile.write('USE_GRID=false\n')
			configfile.write('GRID_NODE=0\n')
			configfile.write('FSA_OL_FILTER_OPTIONS="--max_overhang=-1 --min_identity=-1 --coverage=40"\n')
			configfile.write('FSA_ASSEMBLE_OPTIONS=""\n')
			configfile.write('FSA_CTG_BRIDGE_OPTIONS="--dump --read2ctg_min_identity=80 --read2ctg_min_coverage=4 --read2ctg_max_overhang=500 --read_min_length=5000 --ctg_min_length=1000 --read2ctg_min_aligned_length=5000 --select_branch=best"\n')

		necat_assemble_cmd = "[ -d  {necat_assembly_folder} ] || mkdir -p {necat_assembly_folder}; " \
						"mkdir -p {necat_assembly_log_folder}; cd {necat_assembly_folder}; " \
						"/usr/bin/time -v necat.pl bridge {necat_assemble_config_path} " \
						"2>&1 | tee {necat_assembly_log_folder}necat_assembly.log " \
			.format(necat_assembly_folder=necat_assembly_folder,necat_assemble_config_path=necat_assemble_config_path,
					necat_assembly_log_folder=necat_assembly_log_folder)

		print("****** NOW RUNNING COMMAND ******: " + necat_assemble_cmd)
		run_cmd(necat_assemble_cmd)

		
		'''
		lr_sample_list = os.path.join(os.getcwd(), "sample_list", "lr_samples.lst")
		cmd_necat_seqs = necat_config_generator(lr_sample_list)


		config_folder_path=os.path.join(os.getcwd(), "configuration")
		createFolder(config_folder_path)
		necat_config_path=os.path.join(os.getcwd(), "configuration", "necat.config")
		
		assembly_name=self.assembly_name
		clean_read_list= os.path.join(os.getcwd(), "CleanedReads", "Cleaned_Long_Reads", "clean_read.txt")
		genome_size=self.genome_size
		threads=GlobalParameter().threads



		def necat_config_generator(lrfile):
			lr_cleaned_read_folder = os.path.join(os.getcwd(), "CleanedReads", "Cleaned_Long_Reads" + "/")
			clean_read_list= os.path.join(os.getcwd(), "CleanedReads", "Cleaned_Long_Reads", "clean_read.txt")

			with open(lrfile) as fh:
				with open(clean_read_list, "w") as fo:
					sample_name_list = fh.read().splitlines()
					read_name_suffix = '.fastq'
					read_name_list = [lr_cleaned_read_folder + x + read_name_suffix for x in sample_name_list]
					lr_parse_string = ' '.join(read_name_list)
					fo.write(lr_parse_string)

		
		with open (necat_config_path,'w') as configfile:
			configfile.write('PROJECT={assembly_name}\n'.format(assembly_name=self.assembly_name))
			configfile.write('ONT_READ_LIST={clean_read_list}\n'.format(clean_read_list=clean_read_list))
			configfile.write('GENOME_SIZE={genome_size}\n'.format(genome_size=genome_size))
			configfile.write('THREADS={threads}\n'.format(threads=GlobalParameter().threads))
			configfile.write('MIN_READ_LENGTH=3000/\n')
			configfile.write('OVLP_FAST_OPTIONS="-n 500 -z 20 -b 2000 -e 0.5 -j 0 -u 1 -a 1000"\n')
			configfile.write('OVLP_SENSITIVE_OPTIONS="-n 500 -z 10 -e 0.5 -j 0 -u 1 -a 1000"\n')
			configfile.write('CNS_FAST_OPTIONSC"-a 2000 -x 4 -y 12 -l 1000 -e 0.5 -p 0.8 -u 0"\n')
			configfile.write('CNS_SENSITIVE_OPTIONS="-a 2000 -x 4 -y 12 -l 1000 -e 0.5 -p 0.8 -u 0"\n')
			configfile.write('TRIM_OVLP_OPTIONS="-n 100 -z 10 -b 2000 -e 0.5 -j 1 -u 1 -a 400"\n')
			configfile.write('ASM_OVLP_OPTIONS="-n 100 -z 10 -b 2000 -e 0.5 -j 1 -u 0 -a 400"\n')
			configfile.write('NUM_ITER=2\n')
			configfile.write('CNS_OUTPUT_COVERAGE=45\n')
			configfile.write('CLEANUP=0\n')
			configfile.write('USE_GRID=false\n')
			configfile.write('GRID_NODE=0\n')
			configfile.write('FSA_OL_FILTER_OPTIONS="--max_overhang=-1 --min_identity=-1 --coverage=40"\n')
			configfile.write('FSA_ASSEMBLE_OPTIONS=""\n')
			configfile.write('FSA_CTG_BRIDGE_OPTIONS="--dump --read2ctg_min_identity=80 --read2ctg_min_coverage=4 --read2ctg_max_overhang=500 --read_min_length=5000 --ctg_min_length=1000 --read2ctg_min_aligned_length=5000 --select_branch=best"\n')



		necat_cmd = "[ -d  {necat_assembly_folder} ] || mkdir -p {necat_assembly_folder}; " \
						"mkdir -p {necat_assembly_log_folder}; cd {necat_assembly_folder}; " \
						"/usr/bin/time -v necat.pl bridge {necat_config_path} " \
						"2>&1 | tee {necat_assembly_log_folder}necat_assembly.log " \
			.format(necat_assembly_folder=necat_assembly_folder,necat_config_path=necat_config_path,
					necat_assembly_log_folder=necat_assembly_log_folder)

		print("****** NOW RUNNING COMMAND ******: " + necat_cmd)
		run_cmd(necat_cmd)
		'''


 
