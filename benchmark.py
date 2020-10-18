#!/usr/bin/env python3
import luigi
import sys
import os
import re
import shutil
import psutil
import subprocess
import optparse
from collections import OrderedDict
from sys import exit
import pandas as pd

from tasks.utility.luigiconfig import configureProject

from tasks.readCleaning.rawReadQC import readqc
from tasks.readCleaning.rawReadQC import rawReadsQC
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import cleanReads
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.mecat2_correct import correctPAC
from tasks.readCleaning.necat_correct import correctONT


from tasks.assembly import spades
from tasks.assembly import lightassembler
from tasks.assembly import sparseassembler
from tasks.assembly import discovardenovo
from tasks.assembly import ray
from tasks.assembly import idba_ud
from tasks.assembly import abyss
from tasks.assembly import smartdenovo
from tasks.assembly import soapdenovo
from tasks.assembly import dbg2olc
from tasks.assembly import minia
from tasks.assembly import flye
from tasks.assembly import canu
from tasks.assembly import necat
from tasks.assembly import mecat2
from tasks.assembly import masurca
from tasks.assembly import abruijn
from tasks.assembly import wtdbg2
from tasks.assembly import miniasm
from tasks.assembly import falcon

from tasks.assembly import skesa
from tasks.assembly import haslr
from tasks.assembly import unicycler
from tasks.assembly import platanus
from tasks.assembly import redundans



if __name__ == '__main__':
	luigi.run()

'''
def main():
	totalcpus = psutil.cpu_count()
	threads = (totalcpus-1)
	mem = psutil.virtual_memory()
	maxMemory= int((mem.available/1073741824) -1)

	required="dataDir organismDomain organismNameprojectDir".split()

	parser = optparse.OptionParser()
	parser.add_option('-i', '--dataDir',
				  help="[OPTIONAL] Path to Data Directory, Default 'raw_data'",
				  dest='dataDir',
				  default=(os.path.abspath(os.path.join((os.getcwd()),"raw_data")))
				  )

	parser.add_option('-p', '--projectDir',
				  help="Name of the project directory",
				  dest='projectDir'
				  )

	parser.add_option('-d', '--organismDomain',type='choice',choices=['prokaryote','eukaryote'],
				  help="Domain of the organism. Choose from 'prokaryote' or 'eukaryote' ",
				  dest='organismDomain'
				  )

	parser.add_option('-n', '--organismName',
				  help="Name of the organism [name without space]",
				  dest='organismName'
				  )

	parser.add_option('-o', '--outDir',
				  help="[Optional] Path to symbolic link Data Directory, Default 'raw_data_symlink'",
				  dest='outDir',
				  default=(os.path.abspath(os.path.join((os.getcwd()),"raw_data_symlink")))
				  )


	parser.add_option('-s', '--schedulerPort',
				  help="[Optional Parameter] Scheduler Port Number. default =int[8888] ",
				  type="int",
				  default=8082
				 )

	parser.add_option('-e', '--emailAddress',
				  help="Provide your email address =email[abc@xyz.com]",
				  dest='emailAddress',
				  default="a.n.sarangi@gmail.com"
				 )

	parser.add_option('-t', '--threads',
				  help="[Optional Parameter, ] Number of threads. Default = (total threads -1)",
				  dest='threads',
				  type="int",
				  default = threads)

	parser.add_option('-x', '--maxMemory',
				  help="[Optional Parameter] Maximum Memory in GB. Default = (available memory in GB -1)",
				  dest='maxMemory',
				  type="int",
				  default=maxMemory
				 )

	options,args = parser.parse_args()

	for r in required:
		if options.__dict__[r] is None:
			parser.error("parameter %s required" %r)


	option_dict = vars(options)

	dataDir = option_dict.get('dataDir')
	outDir = option_dict.get('outDir')
	projectName=option_dict.get('projectDir')
	assembly_name=option_dict.get('organismName')
	domain=option_dict.get('organismDomain')

	adapter=os.path.join(os.getcwd(),"tasks","utility",'adapters.fasta.gz')

	email=option_dict.get('emailAddress')
	port = int(option_dict.get('schedulerPort'))
	cpu = int(option_dict.get('threads'))
	memory = int(option_dict.get('maxMemory'))


	def createFolder(directory):
		try:
			if not os.path.exists(directory):
				os.makedirs(directory)
		except OSError:
			print ('Error: Creating directory. ' + directory)


	def run_cmd(cmd):
		p = subprocess.Popen(cmd, bufsize=-1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, executable='/bin/bash')
		output = p.communicate()[0]
		return output



	paired_end_read_dir=os.path.abspath(os.path.join(outDir, "pe" ))
	mate_pair_read_dir=os.path.abspath(os.path.join(outDir,"mp"))
	pacbio_read_dir=os.path.abspath(os.path.join(outDir,"pacbio"))
	ont_read_dir=os.path.abspath(os.path.join(outDir,"ont"))
	projectDir=os.path.abspath(os.path.join(projectName))


	createFolder(paired_end_read_dir)
	createFolder(mate_pair_read_dir)
	createFolder(pacbio_read_dir)
	createFolder(ont_read_dir)
	createFolder(projectDir)
	createFolder("sample_list")

	if os.path.isdir(dataDir):
		files = [f for f in os.listdir(dataDir) if os.path.isfile(os.path.join(dataDir, f))]
		keys = []
		fileList = re.compile(r'^(.+?).(gff|gtf|fasta|fna|ffn|fa|fastq|fq|fastq\.gz|fq\.gz)?$')
		for file in files:
			if fileList.search(file):
				keys.append(file)

	dicts = OrderedDict ()
	#keys = [f for f in os.listdir(".") if os.path.isfile(os.path.join(".", f))]

	for i in keys:
		
		accepted_values_genome="pe mp ont pacbio".split()
		
		val = str(input("Enter Data Type of {data}: \tchoose from [pe:paired-end, mp:mate-pair, ont:nanopore, pacbio:pacbio]: ".format(data=i)))
		if val in accepted_values_genome:
			dicts[i] = val
		else:
			print(f'{val} is not a valid option. \tchoose from [pe, mp, ont, pacbio]: ')
			val = str(input("Enter Data Type of {data}: \tchoose from [pe, mp, ont, pacbio]: ".format(data=i)))

	#print(dicts)

	for key, val in dicts.items():
		if not os.path.exists(os.path.join(outDir, val)):
			os.mkdir(os.path.join(outDir, val))

	##ln -nsf method
	for key, val in dicts.items():
		dest = (os.path.join(outDir,val,key))
		src = (os.path.join(dataDir,key))
		source = os.path.abspath(src)
		destination = os.path.abspath(dest)
		escape="\'"
		print("Source:\t {skip}{src}{skip}".format(skip=escape,src=source))
		print("Destination:\t {skip}{dest}{skip}".format(skip=escape,dest=destination))
		#print("Desitnation:", '\'destination\'')

		link_cmd = "ln -nsf "
		create_symlink = "{link_cmd} {source} {destination}".format(link_cmd=link_cmd,source=source,destination=destination)
		print("****** NOW RUNNING COMMAND ******: " + create_symlink)
		print (run_cmd(create_symlink))

	###########################################
	def paired_end_samples(pe_dir):
		pe_read_list=os.listdir(paired_end_read_dir)

		sample_list=[]
		for read in pe_read_list:
			pe_allowed_extn=["_R1.fq","_R1.fastq","_R1.fq.gz","_R1.fastq.gz"]
			if any (read.endswith(ext) for ext in pe_allowed_extn):
				
				sample_name=read.split("_R1.f",1)[0]
				sample_list.append(sample_name)

				file_extn=read.split('.',1)[1]

		with open ((os.path.join(os.getcwd(),"sample_list",'pe_samples.lst')),'w') as file:
			for sample in sample_list:
				file.write("%s\n" % sample)
		file.close()

		return file_extn

	#############################################
	def mate_pair_samples(pe_dir):
		mp_read_list=os.listdir(mate_pair_read_dir)

		sample_list=[]
		for read in mp_read_list:
			mp_allowed_extn=["_R1.fq","_R1.fastq","_R1.fq.gz","_R1.fastq.gz"]
			if any (read.endswith(ext) for ext in mp_allowed_extn):
				
				sample_name=read.split("_R1.f",1)[0]
				sample_list.append(sample_name)

				file_extn=read.split('.',1)[1]

		with open ((os.path.join(os.getcwd(),"sample_list",'mp_samples.lst')),'w') as file:
			for sample in sample_list:
				file.write("%s\n" % sample)
		file.close()

		return file_extn

	#################################################################################

	def pacbio_samples(pb_dir):
		raw_pb_read_list=os.listdir(pacbio_read_dir)
		sample_list=[]
		for read in raw_pb_read_list:
			raw_pb_allowed_extn=[".fq",".fastq",".fq.gz",".fastq.gz"]
			
			if any (read.endswith(ext) for ext in raw_pb_allowed_extn):
				
				sample_name=read.split(".",1)[0]
				sample_list.append(sample_name)

				file_extn=read.split('.',1)[1]


		with open ((os.path.join(os.getcwd(),"sample_list",'pac_samples.lst')),'w') as file:
			for sample in sample_list:
				file.write("%s\n" % sample)
		file.close()

		return file_extn

	################################################################################
	def ont_samples(ont_raw_dir):
		corr_ont_read_list=os.listdir(ont_read_dir)
		sample_list=[]
		for read in corr_ont_read_list:
			corr_ont_allowed_extn=[".fq",".fastq",".fq.gz",".fastq.gz"]
			
			if any (read.endswith(ext) for ext in corr_ont_allowed_extn):
				
				sample_name=read.split(".",1)[0]
				sample_list.append(sample_name)
				file_extn=read.split('.',1)[1]


		with open ((os.path.join(os.getcwd(),"sample_list",'ont_samples.lst')),'w') as file:
			for sample in sample_list:
				file.write("%s\n" % sample)
		file.close()

		return file_extn

	#Get Read Extension

	if ((len(os.listdir(paired_end_read_dir))!=0)):
		paired_end_read_suffix=paired_end_samples(paired_end_read_dir)


	if ((len(os.listdir(mate_pair_read_dir))!=0)):
		matepair_read_suffix=mate_pair_samples(mate_pair_read_dir)


	if ((len(os.listdir(ont_read_dir))!=0)):
		ont_read_suffix=ont_samples(ont_read_dir)

	if ((len(os.listdir(pac_read_dir))!=0)):
		pac_read_suffix=pacbio_samples(pacbio_read_dir)


	with open('luigi.cfg', 'w') as config:
		config.write('[core]\n')
		config.write('default-scheduler-port:{port}\n'.format(port=port))
		config.write('error-email={email}\n\n'.format(email=email))
		config.write('[GlobalParameter]\n')
		config.write('projectName={projectName}\n'.format(projectName=projectName))
		config.write('assembly_name={assembly_name}\n'.format(assembly_name=assembly_name))
		config.write('projectDir={projectDir}/\n'.format(projectDir=projectDir))
		config.write('adapter={adapter}\n'.format(adapter=adapter))
		config.write('domain={domain}\n'.format(domain=domain))


		#PE READ
		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0) and (len(os.listdir(pacbio_read_dir))==0) and (len(os.listdir(mate_pair_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('seq_platforms=pe\n')
			
			config.write('mp_read_dir=NA\n')
			config.write('mp_read_suffix=NA\n')
			config.write('pac_read_dir=NA\n')		
			config.write('pac_read_suffix=NA\n')
			config.write('ont_read_dir=NA\n')
			config.write('ont_read_suffix=NA\n')


		if ((len(os.listdir(ont_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))==0) and (len(os.listdir(paired_end_read_dir))==0) and (len(os.listdir(pacbio_read_dir))==0)):
			config.write('ont_read_dir={ont_read_dir}/\n'.format(ont_read_dir=ont_read_dir))
			config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
			config.write('seq_platforms=ont\n')

			config.write('mp_read_dir=NA\n')
			config.write('mp_read_suffix=NA\n')
			config.write('pac_read_dir=NA\n')		
			config.write('pac_read_suffix=NA\n')
			config.write('pe_read_dir=NA\n')
			config.write('pe_read_suffix=NA\n')



		if ((len(os.listdir(pacbio_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0) and (len(os.listdir(mate_pair_read_dir))==0) and (len(os.listdir(paired_end_read_dir))==0)):
			config.write('pac_read_dir={pacbio_read_dir}/\n'.format(pacbio_read_dir=pacbio_read_dir))
			config.write('pac_read_suffix={pacbio_read_suffix}\n'.format(pacbio_read_suffix=pacbio_read_suffix))
			config.write('seq_platforms=pac\n')

			config.write('mp_read_dir=NA\n')
			config.write('mp_read_suffix=NA\n')
			config.write('pe_read_dir=NA\n')		
			config.write('pe_read_suffix=NA\n')
			config.write('ont_read_dir=NA\n')
			config.write('ont_read_suffix=NA\n')


		#PE and MP RAED
		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0) and (len(os.listdir(pacbio_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('mp_read_dir={mate_pair_read_dir}/\n'.format(mate_pair_read_dir=mate_pair_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('mp_read_suffix={matepair_read_suffix}\n'.format(matepair_read_suffix=matepair_read_suffix))
			config.write('seq_platforms=pe-mp\n')

			config.write('pac_read_dir=NA\n')		
			config.write('pac_read_suffix=NA\n')
			config.write('ont_read_dir=NA\n')
			config.write('ont_read_suffix=NA\n')

		#PE MP ONT
		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))!=0) and (len(os.listdir(ont_read_dir))!=0) and (len(os.listdir(pacbio_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('mp_read_dir={mate_pair_read_dir}/\n'.format(mate_pair_read_dir=mate_pair_read_dir))
			config.write('ont_read_dir={ont_read_dir}/\n'.format(ont_read_dir=ont_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('mp_read_suffix={matepair_read_suffix}\n'.format(matepair_read_suffix=matepair_read_suffix))
			config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
			config.write('seq_platforms=pe-mp-ont\n')


			config.write('pac_read_dir=NA\n')		
			config.write('pac_read_suffix=NA\n')
		

		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))!=0) and (len(os.listdir(pacbio_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('mp_read_dir={mate_pair_read_dir}/\n'.format(mate_pair_read_dir=mate_pair_read_dir))
			config.write('pac_read_dir={pacbio_read_dir}/\n'.format(pacbio_read_dir=pacbio_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('mp_read_suffix={matepair_read_suffix}\n'.format(matepair_read_suffix=matepair_read_suffix))
			config.write('pac_read_suffix={pacbio_read_suffix}\n'.format(pacbio_read_suffix=pacbio_read_suffix))
			config.write('seq_platforms=pe-mp-pac\n')

			config.write('ont_read_dir=NA\n')
			config.write('ont_read_suffix=NA\n')


		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(ont_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))==0) and (len(os.listdir(pacbio_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('ont_read_dir={ont_read_dir}/\n'.format(ont_read_dir=ont_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
			config.write('seq_platforms=pe-ont\n')

			config.write('mp_read_dir=NA\n')
			config.write('mp_read_suffix=NA\n')
			config.write('pac_read_dir=NA\n')		
			config.write('pac_read_suffix=NA\n')
		
		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(pacbio_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))==0) and (len(os.listdir(ont_read_dir))==0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('pac_read_dir={pacbio_read_dir}/\n'.format(pacbio_read_dir=pacbio_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('pac_read_suffix={pacbio_read_suffix}\n'.format(pacbio_read_suffix=pacbio_read_suffix))
			config.write('seq_platforms=pe-pac\n')

			config.write('mp_read_dir=NA\n')
			config.write('mp_read_suffix=NA\n')
			config.write('ont_read_dir=NA\n')		
			config.write('ont_read_suffix=NA\n')


		if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(mate_pair_read_dir))!=0) and (len(os.listdir(pacbio_read_dir))!=0) and (len(os.listdir(ont_read_dir))!=0)):
			config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
			config.write('mp_read_dir={mate_pair_read_dir}/\n'.format(mate_pair_read_dir=mate_pair_read_dir))
			config.write('pac_read_dir={pacbio_read_dir}/\n'.format(pacbio_read_dir=pacbio_read_dir))
			config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
			config.write('mp_read_suffix={matepair_read_suffix}\n'.format(matepair_read_suffix=matepair_read_suffix))
			config.write('pac_read_suffix={pacbio_read_suffix}\n'.format(pacbio_read_suffix=pacbio_read_suffix))
			config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
			config.write('seq_platforms=pe-mp-pac-ont\n')

		
		config.write('threads={cpu}\n'.format(cpu=cpu)) 
		config.write('maxMemory={memory}\n'.format(memory=memory))
		config.close()

'''

