import luigi
import os
import subprocess
import pyfastx
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.mecat2_correct import correctPAC
from tasks.readCleaning.necat_correct import correctONT

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


class abruijn(luigi.Task):
	projectName = GlobalParameter().projectName
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads  

	seq_platform = luigi.ChoiceParameter(description="Choose From['pacbio, nano]",
									choices=["pacbio", "nano"], var_type=str)

	genome_size = GlobalParameter().genome_size
	threads=GlobalParameter().threads



	def requires(self):
		if self.seq_platform=="pacbio" and self.pre_process_reads=="yes":
			return [correctPAC(pre_process_reads="yes")]

		if self.seq_platform=="pacbio" and self.pre_process_reads=="no":
			return [correctPAC(pre_process_reads="no")]

		if self.seq_platform=="nano" and self.pre_process_reads=="yes":
			return [correctONT(pre_process_reads="yes")]

		if self.seq_platform=="nano" and self.pre_process_reads=="no":
			return [correctONT(pre_process_reads="no")]




	

	def output(self):
		abruijn_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "abruijn_" + self.seq_platform,self.assembly_name +"/")
		return {'out': luigi.LocalTarget(abruijn_assembly_folder + "draft_assembly.fasta")}

	def run(self):
		
		abruijn_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "abruijn_" + self.seq_platform,self.assembly_name +"/")

		abruijn_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "abruijn_",self.assembly_name + "/")


		if self.seq_platform=="pacbio":
			abruijn_input = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
	   
		if self.seq_platform=="nano":
			abruijn_input = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","NECAT",self.assembly_name,"1-consensus","cns_final.fasta")


		def coverage(abruijn_input,genomesize):
			read=pyfastx.Fasta(abruijn_input)

			print('read',read)
			genomelenth = int(genomesize)

			print('genome length',genomelenth)

			total_bases_in_reads=read.size

			print('read_bases',total_bases_in_reads)

			read_coverage=int(total_bases_in_reads/genomelenth)

			return read_coverage




		read_coverage = coverage(abruijn_input,self.genome_size)

		print('read coverage', read_coverage)


		abruijn_cmd = "[ -d  {abruijn_assembly_folder} ] || mkdir -p {abruijn_assembly_folder}; " \
						"[ -d  {abruijn_assembly_log_folder} ] || mkdir -p {abruijn_assembly_log_folder}; " \
						"/usr/bin/time -v abruijn -p {seq_platform} " \
						"-t {threads} " \
						"{abruijn_input} {abruijn_assembly_folder} {read_coverage} " \
						"2>&1 | tee {abruijn_assembly_log_folder}abruijn_assembly.log " \
			.format(abruijn_assembly_log_folder=abruijn_assembly_log_folder,
					seq_platform=self.seq_platform,			
					abruijn_input=abruijn_input,
					abruijn_assembly_folder=abruijn_assembly_folder,
					read_coverage=read_coverage,
					threads=GlobalParameter().threads)

		print("****** NOW RUNNING COMMAND ******: " + abruijn_cmd)
		run_cmd(abruijn_cmd)


 
