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


class miniasm(luigi.Task):
	projectName = GlobalParameter().projectName
	genome_size = GlobalParameter().genome_size
	assembly_name = GlobalParameter().assembly_name
	threads=GlobalParameter().threads
	pre_process_reads=luigi.ChoiceParameter(choices=["yes","no"],var_type=str)
	seq_platform = luigi.ChoiceParameter(description="Choose From['pacbio, nanopore]",
											  choices=["pac", "ont"], var_type=str)

	def requires(self):
		if self.seq_platform  == "pac":
			return [correctPAC(pre_process_reads=self.pre_process_reads)]


		if self.seq_platform == "ont":
			return [correctONT(pre_process_reads=self.pre_process_reads)]


	def output(self):
		miniasm_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "miniasm_" + self.seq_platform,self.assembly_name +"/")
		return {'out': luigi.LocalTarget(miniasm_assembly_folder,GlobalParameter().assembly_name +"_contigs.fasta")}


	def run(self):
		miniasm_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "miniasm_" + self.seq_platform,self.assembly_name +"/")
		
		if self.seq_platform == "pac":
			technology="ava-pb"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
			log_foler= os.path.join(os.getcwd(), "log", "GenomeAssembly", "miniasm_pacbio" +  "/")
		
		if self.seq_platform == "ont":

			technology="ava-ont"
			input_fasta = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
			log_foler= os.path.join(os.getcwd(), "log", "GenomeAssembly", "miniasm_nanopore" +  "/")


		minimap_cmd = "[ -d  {miniasm_assembly_folder} ] || mkdir -p {miniasm_assembly_folder}; cd {miniasm_assembly_folder}; " \
					  "minimap2 -x {technology} -t {threads} {input_fasta} {input_fasta} | gzip > overlaps.paf.gz ".format(technology=technology,miniasm_assembly_folder=miniasm_assembly_folder,
					  	threads=GlobalParameter().threads,
					  	input_fasta=input_fasta)
					  

		miniasm_cmd = "[ -d  {miniasm_assembly_folder} ] || mkdir -p {miniasm_assembly_folder}; " \
						"cd {miniasm_assembly_folder}; " \
						"/usr/bin/time -v miniasm -f {input_fasta} overlaps.paf.gz > contigs.gfa ".format(miniasm_assembly_folder=miniasm_assembly_folder,
																										  input_fasta=input_fasta)
		awk_cmd='''awk '$1 ~/S/ {print ">" $2 "\\n" $3}'	'''			
		#awk_cmd='''awk '/^S/{print">"$2\\n"$3"}' '''
		gfa2fasta_cmd = "[ -d  {miniasm_assembly_folder} ] || mkdir -p {miniasm_assembly_folder}; cd {miniasm_assembly_folder}; " \
						" {awk_cmd} contigs.gfa | fold > {assembly_name}_contigs.fasta".format(miniasm_assembly_folder=miniasm_assembly_folder, 
							awk_cmd=awk_cmd, assembly_name=GlobalParameter().assembly_name)




		print("****** NOW RUNNING COMMAND ******: " + minimap_cmd)
		run_cmd(minimap_cmd)

		print("****** NOW RUNNING COMMAND ******: " + miniasm_cmd)
		run_cmd(miniasm_cmd)

		print("****** NOW RUNNING COMMAND ******: " + gfa2fasta_cmd)
		run_cmd(gfa2fasta_cmd)

