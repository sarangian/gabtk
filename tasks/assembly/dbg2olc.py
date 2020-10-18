#!/usr/bin/env python3
import os
import re
#from tasks.assembly.kmergenie import kmergenie_formater_cleanFastq
#from tasks.assembly.kmergenie import kmergenie_formater_reformat
from tasks.assembly.kmergenie import kmergenie_formater_cleanFastq
from tasks.assembly.kmergenie import kmergenie_formater_reformat
from tasks.assembly.kmergenie import optimal_kmer

from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.assembly.minia import *
from tasks.assembly.sparseassembler import *
from tasks.assembly.lightassembler import *
from tasks.readCleaning.mecat2_correct import correctPAC
from tasks.readCleaning.necat_correct import correctONT


import luigi
import os
import subprocess

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

#minia=os.path.join(os.getcwd(),"GenomeAssembly", "MINIA")

#createFolder(minia)

class dbg2olc(luigi.Task):
    projectName = GlobalParameter().projectName
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
    assembly_name = GlobalParameter().assembly_name
    threads=GlobalParameter().threads
    seq_platforms = luigi.ChoiceParameter(description="Choose From['pac: pacbio, ont:nanopore']",
                                          choices=["pac", "ont"], var_type=str)

    dbg_assembler = luigi.ChoiceParameter(default="sparse",description="Choose From['light: LightAssembler, sparse: SparseAssembler, minia: MiniaAssembler']",
                                          choices=["sparse", "light", "minia"], var_type=str)

    def requires(self):

        #PAC Reads

        if self.seq_platforms=="pac" and self.dbg_assembler=="sparse" and self.pre_process_reads=="yes":
            return [correctPAC(pre_process_reads=self.pre_process_reads),                    
                    sparseAssembler(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="pac" and self.dbg_assembler=="light" and self.pre_process_reads=="yes":
            return [correctPAC(pre_process_reads=self.pre_process_reads),
                    lightAssembler(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="pac" and self.dbg_assembler=="minia" and self.pre_process_reads=="yes":
            return [correctPAC(pre_process_reads=self.pre_process_reads),
                    minia(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]





        if self.seq_platforms=="pac" and self.dbg_assembler=="sparse" and self.pre_process_reads=="no":
            return [correctPAC(pre_process_reads=self.pre_process_reads),
                    sparseAssembler(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="pac" and self.dbg_assembler=="light" and self.pre_process_reads=="no":
            return [correctPAC(pre_process_reads=self.pre_process_reads),
                    lightAssembler(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="pac" and self.dbg_assembler=="minia" and self.pre_process_reads=="no":
            return [correctPAC(pre_process_reads=self.pre_process_reads),
                    minia(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        ##ONT Reads
        if self.seq_platforms=="ont" and self.dbg_assembler=="sparse" and self.pre_process_reads=="yes":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    sparseAssembler(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="ont" and self.dbg_assembler=="light" and self.pre_process_reads=="yes":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    lightAssembler(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="ont" and self.dbg_assembler=="minia" and self.pre_process_reads=="yes":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    minia(pre_process_reads="yes",seq_platforms ="pe"),
                    kmergenie_formater_cleanFastq(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="ont" and self.dbg_assembler=="sparse" and self.pre_process_reads=="no":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    sparseAssembler(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="ont" and self.dbg_assembler=="light" and self.pre_process_reads=="no":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    lightAssembler(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


        if self.seq_platforms=="ont" and self.dbg_assembler=="minia" and self.pre_process_reads=="no":
            return [correctONT(pre_process_reads=self.pre_process_reads),
                    minia(pre_process_reads="no",seq_platforms ="pe"),
                    kmergenie_formater_reformat(os.path.join(os.getcwd(), "sample_list", "pe_samples.lst"))]


    def output(self):
        dbg2olc_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "DBG2OLC_" + self.seq_platforms,self.assembly_name +"/")
        return {'out': luigi.LocalTarget(dbg2olc_assembly_folder + "DBG2OLC_contigs.fa")}

    def run(self):
        
        dbg2olc_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly", "DBG2OLC_" + self.seq_platforms,self.assembly_name +"/")

        DBG2OLC_assembly_log_folder = os.path.join(os.getcwd(), self.projectName,"log", "GenomeAssembly", "DBG2OLC",self.assembly_name + "/")

        
        ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
        pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")
        pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")

        cleand_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","CleanedReads","PE-Reads" + "/")
        verify_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName,"ReadQC","VerifiedReads","PE-Reads" + "/")

        kmergenie_sample_list=os.path.join(os.getcwd(),GlobalParameter().projectName,"GenomeAssembly", "KmerGenie", GlobalParameter().assembly_name, GlobalParameter().assembly_name+".lst")



        if self.seq_platforms=="pac":
            dbg2olc_input = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","MECAT",self.assembly_name,"1-consensus","cns_final.fasta")
       
        if self.seq_platforms=="ont":
            dbg2olc_input = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly","NECAT",self.assembly_name,"1-consensus","cns_final.fasta")
        

        if self.dbg_assembler=="minia" and self.pre_process_reads=="yes":
            dbg_contig_file=os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MINIA", self.assembly_name,self.assembly_name+"_contigs.fa")
            kmer = optimal_kmer(kmergenie_sample_list)

        if self.dbg_assembler=="sparse" and self.pre_process_reads=="yes":
            dbg_contig_file=os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "SparseAssemble_pe", self.assembly_name, self.assembly_name+"_contigs.fasta")
            kmer = optimal_kmer(kmergenie_sample_list)

        if self.dbg_assembler=="light" and self.pre_process_reads=="yes":
            dbg_contig_file=os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "LightAssembler_pe", self.assembly_name, self.assembly_name +"_contigs.fasta")
            kmer = optimal_kmer(kmergenie_sample_list)

        if self.dbg_assembler=="minia" and self.pre_process_reads=="no":
            dbg_contig_file=os.path.join(os.getcwd(), GlobalParameter().projectName,"GenomeAssembly", "MINIA", self.assembly_name, self.assembly_name +".contigs.fa")
            kmer = optimal_kmer(kmergenie_sample_list)

        if self.dbg_assembler=="sparse" and self.pre_process_reads=="no":
            dbg_contig_file=os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "SparseAssemble_pe", self.assembly_name, self.assembly_name+"_contigs.fasta")
            kmer = optimal_kmer(kmergenie_sample_list)

        if self.dbg_assembler=="light" and self.pre_process_reads=="no":
            dbg_contig_file=os.path.join(os.getcwd(),  self.projectName,"GenomeAssembly", "LightAssembler_pe", self.assembly_name, self.assembly_name +"_contigs.fasta")
            kmer = optimal_kmer(kmergenie_sample_list)


        run_cmd_dbg2olc = "[ -d  {dbg2olc_assembly_folder} ] || mkdir -p {dbg2olc_assembly_folder}; " \
                        "mkdir -p {DBG2OLC_assembly_log_folder}; cd {dbg2olc_assembly_folder}; " \
                        "/usr/bin/time -v DBG2OLC " \
                        "k {kmer} Contigs {dbg_contig_file} " \
                        "KmerCovTh 2 MinOverlap 20 AdaptiveTh 0.005 " \
                        "{dbg2olc_input} " \
                        "2>&1 | tee {DBG2OLC_assembly_log_folder}dbg2olc_assembly.log " \
            .format(dbg_contig_file=dbg_contig_file,
                    dbg2olc_assembly_folder=dbg2olc_assembly_folder,
                    dbg2olc_input=dbg2olc_input,
                    DBG2OLC_assembly_log_folder=DBG2OLC_assembly_log_folder,
                    kmer=kmer)
       

        print("****** NOW RUNNING COMMAND ******: " + run_cmd_dbg2olc)
        print(run_cmd(run_cmd_dbg2olc))