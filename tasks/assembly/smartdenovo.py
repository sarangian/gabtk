import luigi
import os
import subprocess
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.reFormatReads import reformat
from tasks.readCleaning.reFormatReads import reformatReads
from tasks.readCleaning.necat_correct import correctONT
from tasks.readCleaning.mecat2_correct import correctPAC

class GlobalParameter(luigi.Config):
	genome_size=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	assembly_name=luigi.Parameter()
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


class smartdenovo(luigi.Task):
	projectName = GlobalParameter().projectName
	genome_size = GlobalParameter().genome_size
	min_contig_size=luigi.Parameter(default="500",description="Minimum contig size")
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	seq_platform = luigi.ChoiceParameter(description="Choose From['pac: pacbio, ont:nanopore']",
										  choices=["pac", "ont"], var_type=str)
	#pre_process_reads=luigi.ChoiceParameter(choices=["yes","no"],var_type=str)
	pre_process_reads=luigi.ChoiceParameter(choices=["yes","no"],var_type=str)


	def requires(self):
		if self.seq_platform=="pac":
			return [correctPAC(pre_process_reads=self.pre_process_reads)]


		if self.seq_platform=="ont":
			return [correctONT(pre_process_reads=self.pre_process_reads)]

	def output(self):
		smartdn_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "SmartDeNovo_" + self.seq_platform,self.assembly_name +"/")
		return {'out': luigi.LocalTarget(smartdn_assembly_folder,self.assembly_name+".dmo.lay")}


	def run(self):

		smartdn_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "SmartDeNovo_" + self.seq_platform,self.assembly_name +"/")
		smartdn_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "SmartDeNovo",self.assembly_name + "/")

		if self.seq_platform=="pac":
			technology="pacbio"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")

		if self.seq_platform=="ont":
			technology="nanopore"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","NECAT",self.assembly_name,"1-consensus","cns_final.fasta")																		
			#log_foler= os.path.join(os.getcwd(), self.projectName, "log", "GenomeAssembly", "SmartDeNovo_nanopore" +  "/")

		
		smartdn_config_cmd = "[ -d  {smartdn_assembly_folder} ] || mkdir -p {smartdn_assembly_folder}; " \
								 "cd {smartdn_assembly_folder}; " \
								 "smartdenovo.pl -p {assembly_name} " \
								 "-t {threads} -J {min_contig_size} " \
								 "{input_fasta} > {assembly_name}_config.mak  " \
						.format(smartdn_assembly_folder=smartdn_assembly_folder,
						input_fasta=input_fasta,
						threads=GlobalParameter().threads,
						min_contig_size=self.min_contig_size,
						assembly_name=self.assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + smartdn_config_cmd)
		run_cmd(smartdn_config_cmd)

		'''

		if self.seq_platforms=="pac":
			log_foler= os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "SmartDeNovo_pacbio" +  "/")
		if self.seq_platforms=="ont":
			log_foler= os.path.join(os.getcwd(), self.projectName, "log", "GenomeAssembly", "SmartDeNovo_nanopore" +  "/")
		'''

		smartdn_run_cmd = "cd {smartdn_assembly_folder}; " \
					"/usr/bin/time -v make -f  {assembly_name}_config.mak  " \
					"2>&1 | tee {smartdn_assembly_folder}wtdbg2_assembly_wtpoa.log " \
			.format(smartdn_assembly_folder=smartdn_assembly_folder,
					assembly_name=GlobalParameter().assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + smartdn_run_cmd)
		run_cmd(smartdn_run_cmd)