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

def run_cmd(cmd):
    p = subprocess.Popen(cmd, bufsize=-1,
                         shell=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         executable='/bin/bash')
    output = p.communicate()[0]
    return output


pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")


def platanus_pe_cleanFastq(samplefile):
    with open(samplefile) as fh:
        sample_name_list = fh.read().splitlines()
        left_read_name_suffix = '_R1.fastq'
        right_read_name_suffix = '_R2.fastq'
        left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
        right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
        pe_cleaned_read_folder = os.path.join(os.getcwd(), "CleanedRead", "Cleaned_PE_Reads" + "/")
        result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
        result1 = [pe_cleaned_read_folder + x + " " + pe_cleaned_read_folder + y + " " for x, y in result]
        parse_string = ' '.join(result1)
        return parse_string
def platanus_pe_reformat(samplefile):
    with open(samplefile) as fh:
        sample_name_list = fh.read().splitlines()
        left_read_name_suffix = '_R1.fastq'
        right_read_name_suffix = '_R2.fastq'
        left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
        right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
        pe_cleaned_read_folder = os.path.join(os.getcwd(), "VerifiedReads", "Verified_PE_Reads" + "/")
        result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
        result1 = [pe_cleaned_read_folder + x + " " + pe_cleaned_read_folder + y + " " for x, y in result]
        parse_string = ' '.join(result1)
        return parse_string



class platanusB(luigi.Task):
    projectName = luigi.Parameter(default="GenomeAssembly")

    seq_platforms = luigi.Parameter(default="pe")
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)

    assembly_name = luigi.Parameter(default="assembly", description="Name of the Assembly")
    kmer=luigi.IntParameter(default=32,description="Initial kmer length for assembly [--kmer 32]")
    steps=luigi.IntParameter(default=10,description="step size of k-mer extension (>= 1, default 10)")
    min_kmer_cov=luigi.IntParameter(default=1,description="Minimun k-mer coverage [--min-kmer-cov 1])")

    def requires(self):
        if self.seq_platforms == "pe" and self.pre_process_reads=="yes":
            return [cleanFastq(seq_platforms="pe",
                          sampleName=i)
                    for i in [line.strip()
                              for line in
                              open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]
        if self.seq_platforms == "pe" and self.pre_process_reads =="no":
            return [[reformat(seq_platforms="pe",
                           sampleName=i)
                     for i in [line.strip()
                               for line in
                               open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]]
    def output(self):
        platanusB_assembly_folder = os.path.join(os.getcwd(), "GenomeAssembly", "PlatanusB" + "/")
        return {'out': luigi.LocalTarget(platanusB_assembly_folder + self.assembly_name + "_contig.fa")}

    def run(self):
        platanusB_assembly_folder = os.path.join(os.getcwd(), "GenomeAssembly", "PlatanusB" + "/")
        platanusB_assembly_log_folder = os.path.join(os.getcwd(), "log", "GenomeAssembly", "PlatanusB" + "/")

        pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")

        if self.pre_process_reads=="yes":
            cmd_platanusB_pe = platanus_pe_cleanFastq(pe_sample_list)
        if self.pre_process_reads=="no":
            cmd_platanusB_pe = platanus_pe_reformat(pe_sample_list)


        run_cmd_platanusB_pe = "[ -d  {platanusB_assembly_folder} ] || mkdir -p {platanusB_assembly_folder}; " \
                           "mkdir -p {platanusB_assembly_log_folder}; cd {platanusB_assembly_folder}; " \
                           "/usr/bin/time -v platanus_b assemble " \
                           "-t {threads} " \
                           "-k {kmer} " \
                           "-s {steps} " \
                           "-c {min_kmer_cov} " \
                           "-m {maxMemory} " \
                           "-o {assemblyName} " \
                           "-f {cmd_platanusB_pe} " \
                           "2>&1 | tee {platanusB_assembly_log_folder}platanus_assembly.log " \
            .format(platanusB_assembly_folder=platanusB_assembly_folder,
                    threads=GlobalParameter().threads,
                    kmer=self.kmer, steps=self.steps,
                    min_kmer_cov=self.min_kmer_cov,
                    maxMemory=GlobalParameter().maxMemory,
                    assemblyName=self.assembly_name,
                    platanusB_assembly_log_folder=platanusB_assembly_log_folder,
                    cmd_platanusB_pe=cmd_platanusB_pe)

        print("****** NOW RUNNING COMMAND ******: " + run_cmd_platanusB_pe)
        run_cmd(run_cmd_platanusB_pe)