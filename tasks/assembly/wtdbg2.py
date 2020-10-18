import luigi
import os
import subprocess
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.necat_correct import correctONT
from tasks.readCleaning.mecat2_correct import correctPAC

class GlobalParameter(luigi.Config):
	projectName=luigi.Parameter()
	genome_size=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	assembly_name=luigi.Parameter()

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


class wtdbg2(luigi.Task):
	projectName = GlobalParameter().projectName
	genome_size = GlobalParameter().genome_size
	min_contig_size=luigi.Parameter(default="500",description="Minimum contog size")
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	pre_process_reads=luigi.ChoiceParameter(choices=["yes","no"],var_type=str)
	seq_platform = luigi.ChoiceParameter(description="Choose From['pacbio raw, nanopore-raw]",
											  choices=["pac", "ont"], var_type=str)


	def requires(self):
		if self.seq_platform=="pac":
			return [correctPAC(pre_process_reads=self.pre_process_reads)]


		if self.seq_platform=="ont":
			return [correctONT(pre_process_reads=self.pre_process_reads)]




	def output(self):
		wtdbg2_assembly_folder = os.path.join(os.getcwd(),self.projectName, "GenomeAssembly", "wtdbg2_" + self.seq_platform,self.assembly_name +"/")
		return {'out': luigi.LocalTarget(wtdbg2_assembly_folder,"1-consensus","bridged_contigs.fasta")}


	def run(self):

		wtdbg2_assembly_folder = os.path.join(os.getcwd(), self.projectName, "GenomeAssembly", "wtdbg2_" + self.seq_platform,self.assembly_name +"/")
		
		if self.seq_platform=="pac":
			technology="rs"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
			log_folder= os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "wtdbg2_pacbio" +  "/")
		
		if self.seq_platform=="ont":
			technology="nanopore"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
			log_folder= os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "wtdbg2_nanopore" +  "/")

		wtdbg2_cmd = "[ -d  {wtdbg2_assembly_folder} ] || mkdir -p {wtdbg2_assembly_folder}; " \
						"mkdir -p {log_folder}; cd {wtdbg2_assembly_folder}; " \
						"/usr/bin/time -v wtdbg2 -i {input_fasta} " \
						"-t {threads} " \
						"-x {technology} " \
						"-p 0 " \
						"-k 15 " \
						"-AS 2 " \
						"-L {min_contig_size} " \
						"-g {genome_size} " \
						"-fo wtdbg2_{assembly_name}_{technology} " \
						"2>&1 | tee {log_folder}wtdbg2_assembly.log " \
			.format(wtdbg2_assembly_folder=wtdbg2_assembly_folder,
					input_fasta=input_fasta,
					threads=GlobalParameter().threads,
					technology=technology,
					log_folder=log_folder,
					min_contig_size=self.min_contig_size,
					genome_size=self.genome_size,
					assembly_name=self.assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + wtdbg2_cmd)
		run_cmd(wtdbg2_cmd)


		wtpoa_cmd = "cd {wtdbg2_assembly_folder}; " \
					"/usr/bin/time -v wtdbg2 " \
					"-t {threads} " \
					"-i wtdbg2_{assembly_name}_{technology}.ctg.lay.gz " \
					"-fo {assembly_name}_contigs.fa " \
					"2>&1 | tee {log_folder}wtdbg2_assembly_wtpoa.log " \
			.format(wtdbg2_assembly_folder=wtdbg2_assembly_folder,
					log_folder=log_folder,
					threads=GlobalParameter().threads,
					technology=technology,
					assembly_name=self.assembly_name)

		print("****** NOW RUNNING COMMAND ******: " + wtpoa_cmd)
		run_cmd(wtpoa_cmd)

