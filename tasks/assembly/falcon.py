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
	assembly_name = luigi.Parameter()
	projectName = luigi.Parameter()

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



class falcon(luigi.Task):
	projectName = GlobalParameter().projectName
	genome_size = GlobalParameter().genome_size
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	min_contig_length = luigi.Parameter(default='500',description="Minimum contig length")
	seq_platform=luigi.ChoiceParameter(description="Choose From['nanopore, pacbio]",
							 choices=["nanopore", "pacbio"], var_type=str)
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	

	def requires(self):
		if self.seq_platform=="pacbio" and self.pre_process_reads=="yes":
			return [filtlong(seq_platforms="pac",sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
		if self.seq_platform=="pacbio" and self.pre_process_reads=="no":
			return [reformat(seq_platforms="pac",sampleName=i)
							for i in [line.strip()for line in
								open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]


		if self.seq_platform=="nanopore" and self.pre_process_reads=="yes":
			return [filtlong(seq_platforms="ont",sampleName=i)
							for i in [line.strip() for line in
								open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
		
		if self.seq_platform=="nanopore" and self.pre_process_reads=="no":
			return [reformat(seq_platforms="ont",sampleName=i)
							for i in [line.strip()for line in
								open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]


	def output(self):
		falcon_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "FALCON_" + self.seq_platform,self.assembly_name +"/")
		return {'out': luigi.LocalTarget(falcon_assembly_folder,"xxxx","XXXX.fasta")}


	def run(self):


		falcon_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "FALCON_" + self.seq_platform,self.assembly_name +"/")
		falcon_assembly_log_folder=os.path.join(os.getcwd(), GlobalParameter().projectName, "log","GenomeAssembly", "FALCON_" + self.seq_platform,self.assembly_name +"/")

		if self.seq_platform == "nanopore":
			lr_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
		if self.seq_platform == "pacbio":
			lr_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")

		if self.seq_platform == "nanopore" and self.pre_process_reads=="no":
			input_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
		if self.seq_platform == "pacbio" and self.pre_process_reads=="no":
			input_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")
		if self.seq_platform == "nanopore" and self.pre_process_reads=="yes":
			input_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
		if self.seq_platform == "pacbio" and self.pre_process_reads=="yes":
			input_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")


		clean_read_list= os.path.join(os.getcwd(), input_read_folder, "input.fofn")


		def falcon_fastq(lrfile,inputDir):
			with open(lrfile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fastq'
				read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
				input_string = ' '.join(read_name_list)
				return input_string

		def falcon_fasta(lrfile,inputDir):
			with open(lrfile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fasta'
				read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
				input_string = ' '.join(read_name_list)
				return input_string

		def falcon_fasta_rename(lrfile,inputDir):
			with open(lrfile) as fh:
				sample_name_list = fh.read().splitlines()
				read_name_suffix = '.fa'
				read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
				input_string = ' '.join(read_name_list)
				return input_string

		def fofn(lrfile,inputDir):
			
			with open(lrfile) as fh:
				with open(clean_read_list, "w") as fo:
					sample_name_list = fh.read().splitlines()
					read_name_suffix = '.fa'
					read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
					input_string = ' '.join(read_name_list)
					fo.write(input_string)



		if self.pre_process_reads=="yes" and self.seq_platform=="pacbio":
			fastq_input = falcon_fastq(lr_sample_list, input_read_folder)
			fasta_out=falcon_fasta(lr_sample_list,input_read_folder)
			fasta_rename=falcon_fasta_rename(lr_sample_list,input_read_folder)
			

		if self.pre_process_reads=="yes" and self.seq_platform=="nanopore":
			fastq_input = falcon_fastq(lr_sample_list, input_read_folder)
			fasta_out=falcon_fasta(lr_sample_list,input_read_folder)
			fasta_rename=falcon_fasta_rename(lr_sample_list,input_read_folder)

		if self.pre_process_reads=="no" and self.seq_platform=="pacbio":
			fastq_input = falcon_fastq(lr_sample_list, input_read_folder)
			fasta_out=falcon_fasta(lr_sample_list,input_read_folder)
			fasta_rename=falcon_fasta_rename(lr_sample_list,input_read_folder)
			

		if self.pre_process_reads=="no" and self.seq_platform=="nanopore":
			fastq_input = falcon_fastq(lr_sample_list, input_read_folder)
			fasta_out=falcon_fasta(lr_sample_list,input_read_folder)
			fasta_rename=falcon_fasta_rename(lr_sample_list,input_read_folder)


		

		fastq2fasta= "cd {input_read_folder}; " \
					 "reformat.sh in={fastq_input} out={fasta_out} " \
		.format(input_read_folder=input_read_folder,fastq_input=fastq_input,
					fasta_out=fasta_out)

		print("****** NOW RUNNING COMMAND ******: " + fastq2fasta)
		run_cmd(fastq2fasta)

		
		cmd_rename_fasta="falcon_rename_fasta.pl {fasta_out} > {fasta_rename}" \
			.format(fasta_out=fasta_out,fasta_rename=fasta_rename)

		print("****** NOW RUNNING COMMAND ******: " + cmd_rename_fasta)
		run_cmd(cmd_rename_fasta)

		fofn(lr_sample_list,input_read_folder)
		
		
		import configparser
		createFolder("configuration")
		config_folder_path=os.path.join(os.getcwd(), "configuration")
		config=configparser.ConfigParser()						
		falcon_cfg_path=os.path.join(config_folder_path,'run_falcon.cfg')
		clean_read_list= os.path.join(os.getcwd(), input_read_folder, "input.fofn")

		genome_size=self.genome_size
		threads=GlobalParameter().threads

		with open (falcon_cfg_path,'w') as configfile:
			configfile.write('[General]\n\n')
			configfile.write('job_type = local \n')
			configfile.write('input_type = raw \n')
			configfile.write('input_fofn = {clean_read_list}\n'.format(clean_read_list=clean_read_list))
			configfile.write('length_cutoff = {length_cutoff}\n'.format(length_cutoff= self.min_contig_length))
			configfile.write('genome_size = {genome_size}\n'.format(genome_size=self.genome_size))

			configfile.write('seed_coverage = 30 \n')
			configfile.write('length_cutoff_pr =  12000\n')
			configfile.write('target = assembly \n')
			configfile.write('jobqueue = all.q \n')
			configfile.write('sge_option_da = -pe smp 8 -q %(jobqueue)s \n')
			configfile.write('sge_option_la = -pe smp 2 -q %(jobqueue)s\n')
			configfile.write('sge_option_pda = -pe smp 8 -q %(jobqueue)s \n')
			configfile.write('sge_option_pla = -pe smp 2 -q %(jobqueue)s\n')
			configfile.write('sge_option_fc = -pe smp 24 -q %(jobqueue)s\n')
			configfile.write('ge_option_cns = -pe smp 8 -q %(jobqueue)s \n')
			configfile.write('pa_concurrent_jobs = 4 \n')
			configfile.write('pa_daligner_option=-e0.8 -l2000 -k18 -h480  -w8 -s100\n')
			configfile.write('ovlp_daligner_option=-e.96 -s1000 -h60 -t32\n')
			configfile.write('ovlp_concurrent_jobs = 8 \n')
			configfile.write('cns_concurrent_jobs = 8 \n')
			configfile.write('pa_HPCdaligner_option =  -v -B128 -t16 -e.70 -l1000 -s1000 \n')
			configfile.write('ovlp_HPCdaligner_option = -v -B128 -t32 -h60 -e.96 -l500 -s1000 \n')
			configfile.write('pa_DBsplit_option = -x500 -s50 \n')
			configfile.write('ovlp_DBsplit_option= -x500 -s50 \n')
			configfile.write('falcon_sense_option = --output-multi --min-idt 0.70 --min-cov 4 --local-match-count-threshold 2 --max-n-read 200 --n-core 10 --output_dformat \n')
			configfile.write('overlap_filtering_setting =  --max-diff 60 --max-cov 90 --min-cov 1 --bestn 10 --n-core {threads}\n'.format(threads=threads))
			#configfile.write(' \n')

		falcon_cmd = "[ -d  {falcon_assembly_folder} ] || mkdir -p {falcon_assembly_folder}; " \
						"mkdir -p {falcon_assembly_log_folder}; cd {falcon_assembly_folder}; " \
						"/usr/bin/time -v fc_run  {falcon_cfg_path} " \
						"2>&1 | tee {falcon_assembly_log_folder}falcon_assembly.log " \
			.format(falcon_assembly_folder=falcon_assembly_folder,
					falcon_assembly_log_folder=falcon_assembly_log_folder,
					falcon_cfg_path=falcon_cfg_path)

		print("****** NOW RUNNING COMMAND ******: " + falcon_cmd)
		run_cmd(falcon_cmd)

