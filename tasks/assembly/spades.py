import luigi
import os
import subprocess
#from tasks.assembly.kmergenie import kmergenie_formater_cleanFastq
#from tasks.assembly.kmergenie import kmergenie_formater_reformat
#from tasks.assembly.kmergenie import optimal_kmer
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import filtlong
from tasks.readCleaning.reFormatReads import reformat

class GlobalParameter(luigi.Config):
    threads = luigi.Parameter()
    maxMemory = luigi.Parameter()
    projectName = luigi.Parameter()
    domain=luigi.Parameter()
    assembly_name=luigi.Parameter()
    pe_read_dir=luigi.Parameter()
    genome_size=luigi.Parameter()

def run_cmd(cmd):
    p = subprocess.Popen(cmd, bufsize=-1,
                         shell=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         executable='/bin/bash')
    output = p.communicate()[0]
    return output

class spades(luigi.Task):
    project_name = GlobalParameter().projectName
    assembly_name=GlobalParameter().assembly_name
    seq_platforms = luigi.ChoiceParameter(description="Choose From['pe: paired-end','pe-mp: paired-end and mate-pair', 'pe-ont: paired-end and nanopore', 'pe-pac: paired-end and pacbio']",
                                    choices=["pe","pe-mp","pe-pac","pe-ont"], var_type=str)
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)

    cov_cutoff = luigi.Parameter(default="off",description="coverage cutoff value (a positive float number, or 'auto', or 'off'). Default 'off'")
    
    kmers = luigi.Parameter(default="auto",description="comma separated list of kmers. must be odd and less than 128. Default 'auto' ")


    def requires(self):
        if self.seq_platforms == "pe" and self.pre_process_reads=="yes":
            return [cleanFastq(seq_platforms="pe",sampleName=i)
                for i in [line.strip()
                          for line in
                          open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

        if self.seq_platforms == "pe" and self.pre_process_reads=="no":
            return [reformat(seq_platforms="pe",sampleName=i)
                for i in [line.strip()
                          for line in
                          open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

        # Paired-end with Mate-pair
        if self.seq_platforms == "pe-mp" and self.pre_process_reads =="yes":
            return [
                        [cleanFastq(seq_platforms="pe",sampleName=i)
                            for i in [line.strip()  for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                        [cleanFastq(seq_platforms="mp",sampleName=i)
                            for i in [line.strip()  for line in
                                open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
                    ]

        if self.seq_platforms == "pe-mp" and self.pre_process_reads =="no":
            return [
                        [reformat(seq_platforms="pe",sampleName=i)
                            for i in [line.strip()  for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                        [reformat(seq_platforms="mp",sampleName=i)
                            for i in [line.strip()  for line in
                                open((os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")))]]
                    ]
        #Paired-end with Pacbio
        if self.seq_platforms == "pe-pac" and self.pre_process_reads == "yes":
            return [
                        [cleanFastq(seq_platforms="pe", sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                        [filtlong(seq_platforms="pac",sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
                    ]


        if self.seq_platforms == "pe-pac" and self.pre_process_reads == "no":
            return [
                        [reformat(seq_platforms="pe", sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]],

                        [reformat(seq_platforms="pac",sampleName=i)
                            for i in [line.strip()for line in
                                open((os.path.join(os.getcwd(), "sample_list", "pac_samples.lst")))]]
                    ]

       
    def output(self):
        spades_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly","SPAdes_" + self.seq_platforms, self.assembly_name +"/")
        return {'out': luigi.LocalTarget(spades_assembly_folder + "scaffolds.fasta")}

    def run(self):
        spades_assembly_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "GenomeAssembly","SPAdes_" + self.seq_platforms, self.assembly_name +"/")
        spades_assembly_log_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "log", "GenomeAssembly","SPAdes",self.assembly_name  + "/")
            
        pe_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
        mp_sample_list = os.path.join(os.getcwd(), "sample_list", "mp_samples.lst")
        se_sample_list = os.path.join(os.getcwd(), "sample_list", "se_samples.lst")
        lr_sample_list = os.path.join(os.getcwd(), "sample_list", "lr_samples.lst")


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

        paired_end='pe'
        mate_pair='mp'
        nanopore_reads='nanopore'
        pacbio_reads='pacbio'
        def spades_illumina(sampleFile,readType,inputDir):
            with open(sampleFile) as fh:
                sample_name_list = fh.read().splitlines()
                left_read_name_suffix = '_R1.fastq'
                left_read_name_prefix = '--'+readType+'1-1 '
                right_read_name_suffix = '_R2.fastq'
                right_read_name_prefix = '--'+readType+'1-2 '

                input_read_folder = inputDir

                left_read_name_list = [input_read_folder + x + left_read_name_suffix for x in sample_name_list]
                left_read_name_list = [left_read_name_prefix + x for x in left_read_name_list]

                right_read_name_list = [input_read_folder + x + right_read_name_suffix for x in sample_name_list]
                right_read_name_list = [right_read_name_prefix + x for x in right_read_name_list]

                input_read_list = [item for sublist in zip(left_read_name_list, right_read_name_list) for item in sublist]

                input_read_string = " ".join(input_read_list)

                return input_read_string


        def spades_longread(sampleFile,libType,inputDir):
            with open(sampleFile) as fh:
                sample_name_list = fh.read().splitlines()
                read_name_suffix = '.fastq'
                read_name_prefix = '--'+libType + ' '

                read_name_list = [inputDir + x + read_name_suffix for x in sample_name_list]
                lr_result = [read_name_prefix + x for x in read_name_list]
                lr_parse_string = " ".join(lr_result)
                return lr_parse_string

      

        if self.pre_process_reads=="yes":
            spades_pe_reads = spades_illumina(pe_sample_list, paired_end, cleaned_pe_read_folder)
            spades_mp_reads = spades_illumina(mp_sample_list, mate_pair, cleaned_mp_read_folder)
            spades_ont_reads = spades_longread(ont_sample_list, nanopore_reads, cleaned_ont_read_folder)
            spades_pac_reads = spades_longread(pac_sample_list, pacbio_reads, cleaned_pac_read_folder)

        if self.pre_process_reads=="no":
            spades_pe_reads = spades_illumina(pe_sample_list, paired_end, verified_pe_read_folder)
            spades_mp_reads = spades_illumina(mp_sample_list, mate_pair, verified_mp_read_folder)
            spades_ont_reads = spades_longread(ont_sample_list, nanopore_reads, verified_ont_read_folder)
            spades_pac_reads = spades_longread(pac_sample_list, pacbio_reads, verified_pac_read_folder)



        spades_pe_cmd = "[ -d  {spades_assembly_folder} ] || mkdir -p {spades_assembly_folder}; " \
                        "mkdir -p {spades_assembly_log_folder}; cd {spades_assembly_folder}; " \
                        "spades.py " \
                        "--threads {threads} " \
                        "--cov-cutoff {cov_cutoff} " \
                        "--memory {maxMemory} " \
                        "-k {kmers} " \
                        "{spades_pe_reads} " \
                        "-o {spades_assembly_folder} " \
                        "2>&1 | tee {spades_assembly_log_folder}spades_assembly.log " \
            .format(spades_assembly_folder=spades_assembly_folder,
                    spades_assembly_log_folder=spades_assembly_log_folder,
                    kmers=self.kmers,
                    cov_cutoff=self.cov_cutoff,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    spades_pe_reads=spades_pe_reads)


        #SPADES PE-MP
        #####################################################################################################
        spades_pe_mp_cmd = "[ -d  {spades_assembly_folder} ] || mkdir -p {spades_assembly_folder}; " \
                           "mkdir -p {spades_assembly_log_folder}; " \
                           "cd {spades_assembly_folder}; " \
                        "spades.py " \
                        "--threads {threads} " \
                        "--cov-cutoff {cov_cutoff} " \
                        "--memory {maxMemory} " \
                        "-k {kmers} " \
                        "{spades_pe_reads} {spades_mp_reads} " \
                        "-o {spades_assembly_folder} " \
                        "2>&1 | tee {spades_assembly_log_folder}spades_assembly.log " \
            .format(spades_assembly_folder=spades_assembly_folder,
                    spades_assembly_log_folder=spades_assembly_log_folder,
                    kmers=self.kmers,
                    cov_cutoff=self.cov_cutoff,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    spades_pe_reads=spades_pe_reads,
                    spades_mp_reads=spades_mp_reads)


        ########################################################################
        #SPADES PE-ONT
        ########################################################################
        spades_pe_ont_cmd = "[ -d  {spades_assembly_folder} ] || mkdir -p {spades_assembly_folder}; " \
                           "mkdir -p {spades_assembly_log_folder};" \
                           "cd {spades_assembly_folder}; " \
                           "spades.py " \
                           "--threads {threads} " \
                           "--cov-cutoff {cov_cutoff} " \
                           "--memory {maxMemory} " \
                           "-k {kmers} " \
                           "{spades_pe_reads} {spades_ont_reads} " \
                           "-o {spades_assembly_folder} " \
                           "2>&1 | tee {spades_assembly_log_folder}spades_assembly.log " \
            .format(spades_assembly_folder=spades_assembly_folder,
                    spades_assembly_log_folder=spades_assembly_log_folder,
                    kmers=self.kmers,
                    cov_cutoff=self.cov_cutoff,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    spades_pe_reads=spades_pe_reads,
                    spades_ont_reads=spades_ont_reads)

        ########################################################################
        #SPADES PE-PAC
        ########################################################################
        spades_pe_pac_cmd = "[ -d  {spades_assembly_folder} ] || mkdir -p {spades_assembly_folder}; " \
                           "mkdir -p {spades_assembly_log_folder};" \
                           "cd {spades_assembly_folder}; " \
                           "spades.py " \
                           "--threads {threads} " \
                           "--cov-cutoff {cov_cutoff} " \
                           "--memory {maxMemory} " \
                           "-k {kmers} " \
                           "{spades_pe_reads} {spades_pac_reads} " \
                           "-o {spades_assembly_folder} " \
                           "2>&1 | tee {spades_assembly_log_folder}spades_assembly.log " \
            .format(spades_assembly_folder=spades_assembly_folder,
                    spades_assembly_log_folder=spades_assembly_log_folder,
                    kmers=self.kmers,
                    cov_cutoff=self.cov_cutoff,
                    threads=GlobalParameter().threads,
                    maxMemory=GlobalParameter().maxMemory,
                    spades_pe_reads=spades_pe_reads,
                    spades_pac_reads=spades_pac_reads)

        ########################################################################
        # RUN
        ########################################################################
        
        if self.seq_platforms == "pe":
            print("****** NOW RUNNING COMMAND ******: " + spades_pe_cmd)
            run_cmd(spades_pe_cmd)


        if self.seq_platforms == "pe-mp":
            print("****** NOW RUNNING COMMAND ******: " + spades_pe_mp_cmd)
            run_cmd(spades_pe_mp_cmd)

        if self.seq_platforms == "pe-ont":
            print("****** NOW RUNNING COMMAND ******: " + spades_pe_ont_cmd)
            run_cmd(spades_pe_ont_cmd)

        if self.seq_platforms == "pe-pac": 
            print("****** NOW RUNNING COMMAND ******: " + spades_pe_pac_cmd)
            run_cmd(spades_pe_pac_cmd)