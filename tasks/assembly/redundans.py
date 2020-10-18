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


 
class redundans(luigi.Task):
    projectName = luigi.Parameter(default="GenomeAssembly")
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
    seq_platforms = luigi.ChoiceParameter(default="pe",choices=["pe","pe-mp","pe-ont","pe-pac"], var_type=str)

    assembly_name = luigi.Parameter(default="assembly", description="Name of the Assembly")


    def requires(self):
        if self.seq_platforms == "pe" and self.pre_process_reads =="yes":
            return [[cleanFastq(seq_platforms=self.seq_platforms,
                           sampleName=i)
                     for i in [line.strip()
                               for line in
                               open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                    ]

        if self.seq_platforms == "pe" and self.pre_process_reads =="no":
            return [[reformat(seq_platforms=self.seq_platforms,
                           sampleName=i)
                     for i in [line.strip()
                               for line in
                               open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                    ]

        if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
            return [
                [cleanFastq(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [cleanFastq(seq_platforms="mp", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
            ]

        if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
            return [
                [reformat(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [reformat(seq_platforms="mp", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
            ]




        if self.seq_platforms == "pe-ont" and self.pre_process_reads == "yes":
            return [
                [cleanFastq(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [filtlong(sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
            ]

        if self.seq_platforms == "pe-ont" and self.pre_process_reads == "no":
            return [
                [reformat(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [reformat(seq_platforms="ont",sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]
            ]


        if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
            return [
                [cleanFastq(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [filtlong(seq_platforms="pac",sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
            ]

        if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
            return [
                [reformat(seq_platforms="pe", sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],


                [reformat(seq_platforms="pac",sampleName=i)
                 for i in [line.strip()
                           for line in
                           open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
            ]


    def output(self):
        redundans_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "redundans_" + self.seq_platforms, self.assembly_name +"/")
        return {'out': luigi.LocalTarget(redundans_assembly_folder + "scaffolds.filled.fa")}


    def run(self):
        redundans_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "redundans_" + self.seq_platforms, self.assembly_name +"/")
        redundans_assembly_log_folder = os.path.join(os.getcwd(), self.projectName, "log", "redundans_" + self.seq_platforms, self.assembly_name +"/")

        pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
        mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
        ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
        pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")


        verified_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
        verified_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "MP-Reads" + "/")
        verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
        verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

        cleaned_pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")
        cleaned_mp_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "MP-Reads" + "/")
        cleaned_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
        cleaned_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")


        def redundans_illumina(samplefile,inDir):
            with open(samplefile) as fh:
                sample_name_list = fh.read().splitlines()
                left_read_name_suffix = '_R1.fastq'
                right_read_name_suffix = '_R2.fastq'
                left_read_name_list = [x + left_read_name_suffix for x in sample_name_list]
                right_read_name_list = [x + right_read_name_suffix for x in sample_name_list]
                pe_cleaned_read_folder = inDir
                result = [sublist for sublist in zip(left_read_name_list, right_read_name_list)]
                result1 = [pe_cleaned_read_folder + x + " " + pe_cleaned_read_folder + y + " " for x, y in result]
                parse_string = ' '.join(result1)
                return parse_string

        def redundans_longread(samplefile,inDir):
            with open(samplefile) as fh:
                sample_name_list = fh.read().splitlines()
                read_name_suffix = '.fastq'
                lr_cleaned_read_folder = inDir
                lr_result = [" --longreads " + lr_cleaned_read_folder + x + read_name_suffix for x in sample_name_list]
                pe_lr_parse_string =  ' '.join(pe_result) + ' '.join(lr_result)
                return pe_lr_parse_string


        if self.pre_process_reads == "yes":
            redundans_pe_reads = redundans_illumina(pe_sample_list, pe_lib, cleaned_pe_read_folder)
            redundans_mp_reads = redundans_illumina(mp_sample_list, mp_lib, cleaned_mp_read_folder)
            redundans_pac_reads = redundans_longread(pac_sample_list, cleaned_pac_read_folder)
            redundans_ont_reads = redundans_longread(ont_sample_list, cleaned_ont_read_folder)

        if self.pre_process_reads == "no":
            redundans_pe_reads = redundans_illumina(pe_sample_list, pe_lib, verified_pe_read_folder)
            redundans_mp_reads = redundans_illumina(mp_sample_list, mp_lib, verified_mp_read_folder)
            redundans_pac_reads = redundans_longread(pac_sample_list, verified_pac_read_folder)
            redundans_ont_reads = redundans_longread(ont_sample_list, verified_ont_read_folder)


        run_cmd_redundans_pe = "[ -d  {redundans_assembly_log_folder} ] || mkdir -p {redundans_assembly_log_folder}; " \
                               "/usr/bin/time -v redundans.py " \
                           "-t {threads} " \
                           "-m {maxMemory} " \
                           "-o {redundans_assembly_folder} " \
                           "-i {cmd_redundans_pe} " \
                           "2>&1 | tee {redundans_assembly_log_folder}redundans_assembly.log " \
            .format(redundans_assembly_folder=redundans_assembly_folder,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    assembly_name=self.assembly_name,
                    redundans_assembly_log_folder=redundans_assembly_log_folder,
                    redundans_pe_reads=redundans_pe_reads)

        run_cmd_redundans_pe_mp = "[ -d  {redundans_assembly_log_folder} ] || mkdir -p {redundans_assembly_log_folder}; " \
                               "/usr/bin/time -v redundans.py " \
                               "-t {threads} " \
                               "-m {maxMemory} " \
                               "-o {redundans_assembly_folder} " \
                               "-i {redundans_pe_reads} {redundans_mp_reads} " \
                               "2>&1 | tee {redundans_assembly_log_folder}redundans_assembly.log " \
            .format(redundans_assembly_folder=redundans_assembly_folder,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    assembly_name=self.assembly_name,
                    redundans_assembly_log_folder=redundans_assembly_log_folder,
                    redundans_pe_reads=redundans_pe_reads,
                    redundans_mp_reads=redundans_mp_reads)

        
        run_cmd_redundans_pe_ont = "[ -d  {redundans_assembly_log_folder} ] || mkdir -p {redundans_assembly_log_folder}; " \
                                     "/usr/bin/time -v redundans.py " \
                                     "-t {threads} " \
                                     "-m {maxMemory} " \
                                     "-o {redundans_assembly_folder} " \
                                     "-i {redundans_pe_reads} {redundans_ont_reads} " \
                                     "2>&1 | tee {redundans_assembly_log_folder}redundans_assembly.log " \
            .format(redundans_assembly_folder=redundans_assembly_folder,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    assembly_name=self.assembly_name,
                    redundans_assembly_log_folder=redundans_assembly_log_folder,
                    redundans_pe_reads=redundans_pe_reads,
                    redundans_ont_reads=redundans_ont_reads)

        run_cmd_redundans_pe_pac = "[ -d  {redundans_assembly_log_folder} ] || mkdir -p {redundans_assembly_log_folder}; " \
                                     "/usr/bin/time -v redundans.py " \
                                     "-t {threads} " \
                                     "-m {maxMemory} " \
                                     "-o {redundans_assembly_folder} " \
                                     "-i {redundans_pe_reads} {redundans_ont_reads} " \
                                     "2>&1 | tee {redundans_assembly_log_folder}redundans_assembly.log " \
            .format(redundans_assembly_folder=redundans_assembly_folder,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    assembly_name=self.assembly_name,
                    redundans_assembly_log_folder=redundans_assembly_log_folder,
                    redundans_pe_reads=redundans_pe_reads,
                    redundans_pac_reads=redundans_pac_reads)


        if self.seq_platforms == "pe":
            print("****** NOW RUNNING COMMAND ******: " + run_cmd_redundans_pe)
            run_cmd(run_cmd_redundans_pe)

        if self.seq_platforms == "pe-mp":
            print("****** NOW RUNNING COMMAND ******: " + run_cmd_redundans_pe_mp)
            run_cmd(run_cmd_redundans_pe_mp)

        if self.seq_platforms == "pe-pac":
            print("****** NOW RUNNING COMMAND ******: " + run_cmd_redundans_pe_pac)
            run_cmd(run_cmd_redundans_pe_pac)

        if self.seq_platforms == "pe-ont":
            print("****** NOW RUNNING COMMAND ******: " + run_cmd_redundans_pe_ont)
            run_cmd(run_cmd_redundans_pe_ont)