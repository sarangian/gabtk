
.. _install:

Installation Instructions
================================

.. toctree::
   :hidden:
  
To install Genome Assembly Benchmark Toolkit, you must have a minimum of 6 GiB free disk space and minimum of 16 GiB free RAM to test run. 

To provide an easier way to install, we provide a miniconda based installer.
Installation also requires **pre-instaled** ``git``, ``gcc``, ``cpp`` and ``zlib1g-dev``.

.. code-block:: none
    
    git clone https://github.com/sarangian/gabtk.git
    cd gabtk
    chmod 755 install.sh
    ./install.sh

    
**Post Installation Instructions**

	
After successful installation, close the current terminal. 
In a new terminal. source the bashrc file:  ``source ~/.bashrc``
Activate ``gabtk`` environment using command: ``conda activate`` 

All the third party tools installed using conda are available at $HOME/gabtk/ [default location]
or the user specified location during the installation process.

The script to run Genome Assembly Benchmark Pipeline is benchmark.py is available inside the gabtk folder, 
that you cloned from github. 



               

