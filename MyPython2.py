import sys
import os
import julia

julia.install()

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
    outfname2 = 'solution2.txt'

# -----------------------------------------------------------------------------
# -- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --------------
# -----------------------------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/Network_01R-10/scenario_1/case.con'
    inl_fname = cwd + r'/Network_01R-10/case.inl'
    raw_fname = cwd + r'/Network_01R-10/scenario_1/case.raw'
    rop_fname = cwd + r'/Network_01R-10/case.rop'
    outfname2 = cwd + r'/solution2.txt'
    try:
        os.remove(outfname2)
    except FileNotFoundError:
        pass

    SFile = open('submission.conf', 'w')
    SFile.write('modules=python/3.7.2\n')
    SFile.write('model=Network_01R-01\n')                   # TODO THIS IS FOR SANDBOX ONLY 1 SCENARIO
    SFile.write('scenario=1\n')
    SFile.close()


cs = julia.Julia()
C2S = cs.include('Code2_Solver.jl')
C2S(con_fname, inl_fname, raw_fname, rop_fname, 600, 2, "IEEE 14")
