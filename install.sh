#!/usr/bin/env bash
set -e
LOGFILE=INSTALL.log
if [ -f $LOGFILE ] ; then
    rm $LOGFILE
fi

# Name of application to install
echo ""
echo -e "\e[1;34mChecking dependencies for GABTk installation ...\e[0m"  2>&1 | tee -a $LOGFILE
echo ""

sleep 2s;
#Check Ubuntu
if [ -f /etc/lsb-release ]; then
    declare -a dpkglist=("git" "zlib1g-dev" "gcc" "cpp")
    for package in "${dpkglist[@]}";
	    do
  		    if [ $(dpkg-query -W -f='${Status}' $package 2>/dev/null | grep -c "ok installed") -eq 1 ];
			    then
  			    echo -e "\e[1;36m $package \t...OK...\tAvailable in PATH \e[0m";  2>&1 | tee -a $LOGFILE
		    else
  			    echo -e "\e[1;31m $package \tNot Available in PATH \e[0m";  2>&1 | tee -a $LOGFILE
			    echo -e "\e[1;33m You need to install $package using: \"sudo apt-get install $package\" \e[0m";  2>&1 | tee -a $LOGFILE 
		    fi
	    done
fi

if [ -f /etc/lsb-release ]; then
    declare -a dpkglist=("git" "zlib1g-dev" "gcc" "cpp")
    for package in "${dpkglist[@]}";
	    do
  		    if ! [ $(dpkg-query -W -f='${Status}' $package 2>/dev/null | grep -c "ok installed") -eq 1 ];
			    then
  			    exit 0
		    fi
	    done
fi


if [ -f /etc/redhat-release ]; then
    declare -a dpkglist=("git" "zlib-devel" "gcc" "gcc-c++")
    for package in "${dpkglist[@]}";
	    do
  		    if [ $(rpm -qa | grep $package 2>/dev/null | grep -c $package) -ge 1 ] ;
			    then
  			    echo -e "\e[1;36m $package \t...OK....\tAvailable in PATH \e[0m";  2>&1 | tee -a $LOGFILE
		    else
  			    echo -e "\e[1;31m $package \tNot Available in PATH \e[0m";  2>&1 | tee -a $LOGFILE
			    echo -e "\e[1;33m You need to install $package using: \"sudo yum install $package\" \e[0m";  2>&1 | tee -a $LOGFILE 
	            fi
	    done
fi

if [ -f /etc/redhat-release ]; then
    declare -a dpkglist=("git" "zlib-devel" "gcc" "gcc-c++")
    for package in "${dpkglist[@]}";
	    do
  		    if ! [ $(rpm -qa | grep $package 2>/dev/null | grep -c $package) -ge 1 ] ;
			    then
  			    exit 0
	            fi
	    done
fi

#Packages through git, wget and Sourseforge
THIS_DIR=$(DIRNAME=$(dirname "$0"); cd "$DIRNAME"; pwd)
BASE_DIR=$(pwd)
chmod 755 $BASE_DIR/*.sh
#chmod 755 $BASE_DIR/*.py
UTILITY=$BASE_DIR/gabtk/tasks/utility
ASSEMBLY=$BASE_DIR/gabtk/tasks/assembly
READCleaning=$BASE_DIR/gabtk/tasks/readCleaning

echo "export PATH=\"$UTILITY\":\$PATH" >> ~/.bashrc 
echo "export PATH=\"$ASSEMBLY\":\$PATH" >> ~/.bashrc 
echo "export PATH=\"$READCleaning\":\$PATH" >> ~/.bashrc 

TEMPENVFILE=$BASE_DIR/.conda_environment.yml
ENVFILE=$BASE_DIR/.environment.yml
cp $TEMPENVFILE $ENVFILE
ENVFILE=$BASE_DIR/.environment.yml

THIS_FILE=$(basename "$0")
THIS_PATH="$THIS_DIR/$THIS_FILE"
PREFIX=$HOME/GABTk/

sleep 2s;

echo -e "\e[1;34m__________________________________DISK USE SUMMARY______________________________\e[0m"  2>&1 | tee -a $LOGFILE
total=$(df --total | tail -n 1 | awk '{print $2}')
used=$(df --total | tail -n 1 | awk '{print $3}')
available=$(df --total | tail -n 1 | awk '{print $4}') 

echo -e "Total Disk Space:\t $(( ${total} / 1024 /1024 )) GB"  2>&1 | tee -a $LOGFILE
echo -e "Used  Disk Space:\t $(( ${used} / 1024 /1024 )) GB"  2>&1 | tee -a $LOGFILE
echo -e "Available Disk Space:\t  $(( ${available} / 1024 /1024 )) GB"  2>&1 | tee -a $LOGFILE
echo -e "\e[1;34m__________________________________CPU SUMMARY____________________________________\e[0m"  2>&1 | tee -a $LOGFILE
lscpu | egrep 'CPU\(s\)|Thread|Model name'


echo -e "\e[1;34m__________________________________Memory USE SUMMARY_____________________________\e[0m"  2>&1 | tee -a $LOGFILE
totalM=$(free -m | grep "Mem:"| awk '{print $2}')
usedM=$(free -m | grep "Mem:"| awk '{print $3}')
availableM=$(free -m | grep "Mem:"| awk '{print $4}')
echo -e "Total Memory:\t $(( ${totalM} )) MB OR $(( ${totalM} / 1024 )) GB"  2>&1 | tee -a $LOGFILE
echo -e "Used  Memory:\t $(( ${usedM} )) MB OR $(( ${usedM} / 1024 )) GB"  2>&1 | tee -a $LOGFILE
echo -e "Available Memory:\t  $(( ${availableM} )) MB OR $(( ${availableM} / 1024 )) GB"  2>&1 | tee -a $LOGFILE

echo -e "\e[1;34m_________________________________________________________________________________\e[0m"  2>&1 | tee -a $LOGFILE
echo -e "Hello "$USER""  2>&1 | tee -a $LOGFILE
    printf "Installation of Genome Assembly Benchmark Toolkit (GABTk) requires at least 2.5 gb free disk space.\\nIf you do not have sufficient disc space, Press CTRL-C to abort the installation.\\n\\nRNASeq Workflow will now be installed into this location:\\n"  2>&1 | tee -a $LOGFILE
    printf "\\n"  2>&1 | tee -a $LOGFILE
    printf "%s\\n" "$PREFIX"  2>&1 | tee -a $LOGFILE
    printf "\\n"  2>&1 | tee -a $LOGFILE
    printf "  - Press ENTER to confirm the location\\n"  2>&1 | tee -a $LOGFILE
    printf "  - Press CTRL-C to abort the installation\\n"  2>&1 | tee -a $LOGFILE
    printf "  - Or specify a different location below\\n"  2>&1 | tee -a $LOGFILE
    printf "\\n"
    printf "[%s] >>> " "$PREFIX"
    read -r user_prefix
    if [ "$user_prefix" != "" ]; then
        case "$user_prefix" in
            *\ * )
                printf "ERROR: Cannot install into directories with spaces\\n" >&2
                exit 1
                ;;
            *)
                eval PREFIX="$user_prefix"
                ;;
        esac
    fi

case "$PREFIX" in
    *\ * )
        printf "ERROR: Cannot install into directories with spaces\\n" >&2
        exit 1
        ;;
esac

if ! mkdir -p "$PREFIX"; then
    printf "ERROR: Could not create directory: '%s'\\n" "$PREFIX" >&2
    exit 1
fi

PREFIX=$(cd "$PREFIX"; pwd)
export PREFIX

printf "PREFIX=%s\\n" "$PREFIX"
if [ ! -d $PREFIX ]; then
  mkdir -p $PREFIX
fi
#cd $PREFIX

AppName="GABTk"
# Set your project's install directory name here
InstallDir=$PREFIX
#EntryPoint="YourApplicationName"
EntryPoint="GABTk"

echo
echo "Installing $AppName"  

echo
echo "Installing into: $InstallDir"
echo


# Test if new directory is empty.  Exit if it's not
if [ -d $InstallDir ]; then
    if [ "$(ls -A $InstallDir)" ]; then
        echo "ERROR: Directory is not empty" >&2
        echo "If you want to install into $InstallDir, "
        echo "clear the directory first and run this script again."
        echo "Exiting..."
        echo
        exit 1
    fi
fi

# Download and install Miniconda
set +e
wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O Miniconda_Install.sh  2>&1 | tee -a $LOGFILE
if [ $? -ne 0 ]; then
    wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O Miniconda_Install.sh  2>&1 | tee -a $LOGFILE
fi
set -e

bash Miniconda_Install.sh -b -f -p $InstallDir
rm Miniconda_Install.sh
# Activate the new environment
PATH="$InstallDir/bin":$PATH

# Make the new python environment completely independent
# Modify the site.py file so that USER_SITE is not imported
python -s << END
import site
site_file = site.__file__.replace(".pyc", ".py");
with open(site_file) as fin:
    lines = fin.readlines();
for i,line in enumerate(lines):
    if(line.find("ENABLE_USER_SITE = None") > -1):
        user_site_line = i;
        break;
lines[user_site_line] = "ENABLE_USER_SITE = False\n"
with open(site_file,'w') as fout:
    fout.writelines(lines)
END

#Add Script Directory

#Add Prefix to env file
echo "prefix: $PREFIX" >> $ENVFILE


#cp -ar $UTILITY $PREFIX
#utility_dir=$PREFIX/utility
#chmod -R 755 $utility_dir
#echo "export PATH=\$PATH:$utility_dir" >> ~/.bashrc

#cp -ar $DEA_SCRIPTS $PREFIX
#dea_scripts_dir=$PREFIX/deaRscripts
#chmod -R 755 $dea_scripts_dir
#echo "export PATH=\$PATH:$dea_scripts_dir" >> ~/.bashrc


# Add Entry Point to the path
if [[ $EntryPoint ]]; then

    cd $InstallDir
    mkdir Scripts
    ln -s ../bin/$EntryPoint Scripts/$EntryPoint

    echo "$EntryPoint script installed to $(pwd)/Scripts"  2>&1 | tee -a $LOGFILE
    echo "export PATH=\"$(pwd)/Scripts\":\$PATH" >> ~/.bashrc
fi




#Install tools from conda
conda env update --prefix $PREFIX --file $ENVFILE  --prune


conda create -n abruijn -c bioconda -y python=2.7 abruijn
abruijn_dir="$InstallDir/envs/abruijn/bin"
echo "export PATH=\"$abruijn_dir\":\$PATH" >> ~/.bashrc 


# Cleanup
conda clean -iltp --yes 2>&1 | tee -a $LOGFILE

source $InstallDir/etc/profile.d/conda.sh
$InstallDir/bin/conda init bash
echo
echo -e "\e[1;36m $AppName Installed Successfully !!!!!!\e[0m";2>&1 | tee -a $LOGFILE
echo ""
echo -e "\e[1;31m ****Post Installation Instructions****\e[0m"; 2>&1 | tee -a $LOGFILE
echo -e "\e[1;36m \t[1]\tRestart the terminal first.  \e[0m";2>&1 | tee -a $LOGFILE
echo -e "\e[1;36m \t[2]\tIn the new terminal, source your .bashrc file using command: source ~/.bashrc  \e[0m"; 2>&1 | tee -a $LOGFILE
echo -e "\e[1;36m \t[3]\tActivate conda environment using command: conda activate \e[0m"; 2>&1 | tee -a $LOGFILE
echo -e "\e[1;36m Have a great day --STLab Team \e[0m"; 2>&1 | tee -a $LOGFILE

