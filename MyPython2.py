import sys
import os
import time
import julia

cwd = os.path.dirname(__file__)
print()

# -----------------------------------------------------------------------------
# -- USING COMMAND LINE -------------------------------------------------------
# -----------------------------------------------------------------------------
if sys.argv[1:]:
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]

# -----------------------------------------------------------------------------
# -- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --------------
# -----------------------------------------------------------------------------
if not sys.argv[1:]:
    NFile = open('network_scenario_config.txt', 'r')
    network_scenario = NFile.read().splitlines()
    NFile.close()
    network, scenario, scenario_num = network_scenario
    raw_fname = cwd + r'/' + network + r'/' + scenario + r'/case.raw'
    con_fname = cwd + r'/' + network + r'/' + scenario + r'/case.con'
    inl_fname = cwd + r'/' + network + r'/case.inl'
    rop_fname = cwd + r'/' + network + r'/case.rop'
    # inl_fname = cwd + r'/' + network + r'/' + scenario + r'/case.inl'
    # rop_fname = cwd + r'/' + network + r'/' + scenario + r'/case.rop'

    outfname = cwd + '//solution2.txt'
    try:
        os.remove(outfname)
    except FileNotFoundError:
        pass
    print('===================  {0:14s}  {1:10s}  ==================='.format(network, scenario))

    # -- WRITE SUBMISSION.CONF FILE ---------------------------------------------------------------
    SFile = open('submission.conf', 'w')
    SFile.write('modules=python/3.7.2\n')

    SFile.write('model=Network_03R-01\n')                 # TODO FOR SANDBOX ONLY
    SFile.write('scenario=1\n')                           # TODO FOR SANDBOX ONLY  delete these reading from network_scenario.config.txt

    # SFile.write('model=' + network + '\n')                  # TODO FOR SANDBOX ONLY
    # SFile.write('scenario=' + scenario_num + '\n')          # TODO FOR SANDBOX ONLY
    SFile.write('export PATH="$GUROBI_811_HOME/bin:$PATH"\n')
    SFile.write('export LD_LIBRARY_PATH="$GUROBI_811_HOME/lib:$LD_LIBRARY_PATH"\n')
    SFile.write('export GRB_LICENSE_FILE="$GUROBI_811_HOME/license/gurobi_client.lic"\n')
    SFile.write('srun_options2=-N6\n')
    SFile.write('alloc_method=salloc\n')
    SFile.write('export JULIA_DEPOT_PATH=$JULIA_DEPOT_PATH_110_CARLETON\n')
    SFile.write('export PATH=$JULIA_110:$PATH\n')
    SFile.write('export LD_LIBRARY_PATH=$APPS_BASE/Ipopt-3.12.13-gcc520/lib:$LD_LIBRARY_PATH\n')
    SFile.write('export PATH=$APPS_BASE/Ipopt-3.12.13-gcc520/bin:$PATH\n')
    SFile.write('modules=gcc/5.2.0\n')
    SFile.close()

start_time = time.time()
# julia.install()
cs = julia.Julia()
C2S = cs.include('Code2_Solver.jl')
C2S(con_fname, inl_fname, raw_fname, rop_fname, output_dir="")
print()
print('SOLUTION2 FILE GENERATED ...............................................', round(time.time() - start_time, 3))

# == DEVELOPEMENT, COPY FILES FOR EVALUATION ------------------------------------------------
if not sys.argv[1:]:
    import shutil
    dirname = os.path.dirname(__file__)
    shutil.copy(outfname, os.path.join(dirname, 'GitHub_Work'))
    shutil.copy('submission.conf', os.path.join(dirname, 'GitHub_Work'))
    shutil.copy('Code2_Solver.jl', os.path.join(dirname, 'GitHub_Work'))
    shutil.copy(os.path.realpath(__file__), os.path.join(dirname, 'GitHub_Work/MyPython2.py'))
