#__init__.py

#Version

__version__="1.0.0"


import os
import sys
current_folder=os.path.join(os.getcwd())
luigi_config=os.path.join(os.getcwd(),"luigi.cfg")

if not os.path.isfile(luigi_config):
	print(f'Please prepare the luigi configuration file first')
	sys.exit(0)
'''

if not luigi_config:
with open(luigi_config, 'w') as config:
	config.write('[core]\n')
	config.write('default-scheduler-port:8082\n')
	config.write('error-email=user_email_id\n\n')
	config.write('[GlobalParameter]\n')
	config.write('projectName=YourProjectName\n')
	config.write('assembly_name=name_of_the_organism\n')
	config.write('projectDir=/location/of/the/project/directorty\n')
	config.write('adapter=/location/of/the/adapter/database/\n')
	config.write('domain=domain_of_the_organosm\n')
	config.write('genome_size=1000000\n')
	config.write('pe_read_dir=/location/of/the/paired-end-reads/\n')
	config.write('mp_read_dir=/location/of/the/mate-pair-reads/\n')
	config.write('pac_read_dir=/location/of/the/pacboio-reads/\n')
	config.write('ont_read_dir=/location/of/the/nanopore-reads/\n')
	config.write('pe_read_suffix=paired-end-reads-extension\n')
	config.write('mp_read_suffix=mate-pair-reads-extension\n')
	config.write('ont_read_suffix=ont_read_extension\n')
	config.write('pac_read_suffix=pacbio_read_extension\n')
	config.write('seq_platforms=read_platform_used\n')
	config.write('threads=6\n')
	config.write('maxMemory=20\n')

	config.close()
print("the luigi config file generated")
'''