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



class flye(luigi.Task):

    projectName = GlobalParameter().projectName
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
    assembly_name = GlobalParameter().assembly_name
    threads=GlobalParameter().threads  

    seq_platform = luigi.ChoiceParameter(description="Choose From['pacbio raw, pacbio corrected, nanopore raw, nanopore corrected']",
                                             choices=["pacbio-raw", "pacbio-corr", "nano-raw", "nano-corr"], var_type=str)

    genome_size = GlobalParameter().genome_size

    threads=GlobalParameter().threads



    def requires(self):
        if self.seq_platform=="pacbio-raw" and self.pre_process_reads=="yes":
            return [filtlong(seq_platforms="pac",sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]

        if self.seq_platform=="nano-raw" and self.pre_process_reads=="yes":
            return [filtlong(seq_platforms="ont",sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]

        if self.seq_platform=="pacbio-corr" and self.pre_process_reads=="no":
            return [reformat(seq_platforms="pac",sampleName=i)
                            for i in [line.strip()for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]

        if self.seq_platform=="nano-corr" and self.pre_process_reads=="no":
            return [reformat(seq_platforms="ont",sampleName=i)
                            for i in [line.strip()for line in
                                open((os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")))]]


    def output(self):
        flye_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "FLYE_" + self.seq_platform, self.assembly_name + "/")
        return {'out': luigi.LocalTarget(flye_assembly_folder + "assembly.fasta")}

    def run(self):

        flye_assembly_folder = os.path.join(os.getcwd(), self.projectName,"GenomeAssembly", "FLYE_" + self.seq_platform, self.assembly_name + "/")

        flye_assembly_log_folder = os.path.join(os.getcwd(),self.projectName, "log", "GenomeAssembly", "Flye_" + self.seq_platform +  "/")
        
        ont_sample_list = os.path.join(os.getcwd(), "sample_list", "ont_samples.lst")
        pac_sample_list = os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")

        verified_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
        verified_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")
        cleaned_ont_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
        cleaned_pac_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PAC-Reads" + "/")


        def flye_formater(lrfile,inputDir):
            with open(lrfile) as fh:
                sample_name_list = fh.read().splitlines()
                read_name_suffix = '.fastq'
                read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
                lr_parse_string = ' '.join(read_name_list)
                return lr_parse_string

        if self.pre_process_reads=="yes" and self.seq_platform=="pacbio-raw":
            input_long_reads = flye_formater(pac_sample_list, cleaned_pac_read_folder)

        if self.pre_process_reads=="yes" and self.seq_platform=="nano-raw":
            input_long_reads = flye_formater(pac_sample_list, cleaned_ont_read_folder)


        if self.pre_process_reads=="no" and self.seq_platform=="nano-corr":
            input_long_reads = flye_formater(pac_sample_list, verified_ont_read_folder)

        if self.pre_process_reads=="no" and self.seq_platform=="pacbio-corr":
            input_long_reads = flye_formater(pac_sample_list, verified_pac_read_folder)

    

        flye_cmd = "[ -d  {flye_assembly_folder} ] || mkdir -p {flye_assembly_folder}; " \
                        "mkdir -p {flye_assembly_log_folder}; cd {flye_assembly_folder}; " \
                        "/usr/bin/time -v flye " \
                        "--{seq_platform} " \
                        "{input_long_reads} " \
                        "--threads {threads} " \
                   "--genome-size {genomeSize} " \
                   "--out-dir {flye_assembly_folder} " \
                   "2>&1 | tee {flye_assembly_log_folder}flye_assembly.log " \
            .format(flye_assembly_folder=flye_assembly_folder,
                    seq_platform=self.seq_platform,input_long_reads=input_long_reads,
                    flye_assembly_log_folder=flye_assembly_log_folder,
                    genomeSize=self.genome_size,
                    threads=GlobalParameter().threads)

        print("****** NOW RUNNING COMMAND ******: " + flye_cmd)
        run_cmd(flye_cmd)


