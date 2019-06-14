import os
import sys
import math
import time
import copy
import pickle
import csv
import numpy
import pandapower as pp
from pandas import options as pdoptions
import multiprocessing

cwd = os.path.dirname(__file__)

# -----------------------------------------------------------------------------
# -- USING COMMAND LINE -------------------------------------------------------
# -----------------------------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    outfname1 = 'solution1.txt'
    outfname2 = 'solution2.txt'
    # neta_fname = os.path.join('submission', 'neta.p')
    # netc_fname = os.path.join('submission', 'netc.p')
    # data_fname = os.path.join('submission', 'netdata.pkl')
    neta_fname = os.path.abspath('..') + r'/neta.p'
    netc_fname = os.path.abspath('..') + r'/netc.p'
    data_fname = os.path.abspath('..') + r'/netdata.pkl'

# -----------------------------------------------------------------------------
# -- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --------------
# -----------------------------------------------------------------------------
if not sys.argv[1:]:
    outfname2 = cwd + r'/sandbox/Network_01R-10/scenario_1/solution2.txt'
    neta_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/neta.p'
    netc_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netc.p'
    data_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netdata.pkl'

    try:
        os.remove(outfname2)
    except FileNotFoundError:
        pass

Multiprocessing = False

# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def write_csvdata(lol, label, writer):
    for j in label:
        writer.writerow(j)
    writer.writerows(lol)
    return


# def write_csvdata(fname, lol, label):
#     with open(fname, 'a', newline='') as fobject:
#         writer = csv.writer(fobject, delimiter=',', quotechar='"')
#         for j in label:
#             writer.writerow(j)
#         writer.writerows(lol)
#     # fobject.close()
#     return


def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus, clabel, writer):
    # -- DELETE UNUSED DATAFRAME COLUMNS --------------------------------------
    try:
        del b_results['p_mw']        # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['q_mvar']      # not used for reporting
    except KeyError:
        pass
    # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
    b_results.drop([exgridbus], inplace=True)
    # -- ADD BUSNUMBER COLUMN -------------------------------------------------
    b_results.insert(0, 'bus', b_results.index)
    # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
    b_results['shunt_b'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage_pu', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage_pu'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus_ = buslist[j][0]
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / b_results.loc[bus_, 'voltage_pu'] ** 2
            buslist[j][3] = mvar_ + 0.0

    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata([], [['--contingency'], ['label'], [clabel]], writer)
    write_csvdata(buslist, [['--bus section']], writer)
    return


def write_gen_results(fname, g_results, genids, gbuses, delta, ssh_idxs, writer):
    g_results.drop(ssh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    g_results.rename(columns={'p_mw': 'mw', 'q_mvar': 'mvar'}, inplace=True)
    # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
    g_results.insert(0, 'id', genids)
    g_results.insert(0, 'bus', gbuses)
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(glist, [['--generator section']], writer)
    write_csvdata([], [['--delta section'], ['delta_p'], [delta]], writer)
    return


def run_gen_outages(genkey, c_net, a_net, base_pgens, outage_dict, area_participating_gens, pfactor_dict, gen_dict,
                    genbus_dict, busarea_dict, ext_grid_idx, swinggen_idxs, alt_sw_genkey, tie_idx, alt_tie_idx, multiprocessing):
    """ run outaged generators and calculate delta variable """
    gnet = copy.deepcopy(c_net)                                                                     # get fresh copy of contingency network
    genidx = gen_dict[genkey]                                                                       # get outaged generator index
    genbus = genbus_dict[genidx]
    outage_area = busarea_dict[genbus]
    participating_units = area_participating_gens[outage_area]
    conlabel = outage_dict['gen'][genkey]                                                           # get contingency label
    gen_outage_p = base_pgens[genkey]                                                               # get outaged generator pgen
    gnet.gen.in_service[genidx] = False                                                             # switch off outaged generator

    if genidx in swinggen_idxs and len(swinggen_idxs) == 1:                                         # check if outage is a swing generator
        gnet.line.loc[tie_idx, 'in_service'] = False                                                # open swing tie
        gnet.line.loc[alt_tie_idx, 'in_service'] = True                                             # close alternate swing tie
        alt_sw_gidx = gen_dict[alt_sw_genkey]                                                       # get alternate swing generator index
        alt_swbus = genbus_dict[alt_sw_gidx]                                                        # get alternate swingbus index
        alt_swvreg = gnet.gen.loc[alt_sw_gidx, 'vm_pu']                                             # get alternate swing generator voltage setpoint
        alt_swangle = gnet.res_bus.loc[alt_swbus, 'va_degree']                                      # get alternate swingbus angle
        gnet.ext_grid.loc[ext_grid_idx, 'vm_pu'] = alt_swvreg                                       # set extgrid voltage setpoint
        gnet.ext_grid.loc[ext_grid_idx, 'va_degree'] = alt_swangle                                  # set extgrid reference angle
        pp.runpp(gnet, init='flat', enforce_q_lims=True)

    try:                                                                                            # try straight power flow
        pp.runpp(gnet, enforce_q_lims=True)                                                         # run straight power flow
    except:                                                                                         # if not solved... write basecase results as placeholder
        print('GEN {0:6s} DID NOT SOLVE WITH POWERFLOW ............................ '.format(genkey))
        delta = 0.0                                                                                 # assign delta = 0
        _bus_results = copy.deepcopy(a_net.res_bus)                                                 # get basecase bus results
        _gen_results = copy.deepcopy(a_net.res_gen)                                                 # get basecase generator results
        return conlabel, _bus_results, _gen_results, delta                                          # return 'dummy' basecase results placeholder

    # -- FIRST ESTIMATE OF DELTA VARIABLE ---------------------------------------------------------
    pfactor_total = 0.0                                                                             # initialize participation factors total
    for gen_pkey in participating_units:                                                             # loop across participating generators
        if gen_pkey == genkey:                                                                      # don't want pfactor for outaged generator
            continue                                                                                # get next participating generator
        pfactor_total += pfactor_dict[gen_pkey]                                                     # increment pfactor total
    pp.runpp(gnet, enforce_q_lims=True)                                                             # run straight power flow
    ex_pgen = gnet.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                           # get extgrid real power
    delta_init = (gen_outage_p + ex_pgen) / pfactor_total                                           # initial delta variable estimate

    # -- ITERATE TO FIND OPTIMUM GENERATOR OUTAGE DELTA VARIABLE  ----------------------------------
    step = 1                                                                                        # initialize iterator
    delta = delta_init                                                                              # set initial delta variable
    net = copy.deepcopy(gnet)                                                                       # get copy of outaged network
    while step < 120:                                                                               # limit while loops
        for gen_pkey in participating_units:                                                         # loop through participating generators
            if gen_pkey == genkey:                                                                  # if generator = outaged generator
                continue                                                                            # get next generator
            gidx = gen_dict[gen_pkey]                                                               # get this generator index
            pgen_a = base_pgens[gen_pkey]                                                           # this generators ratea basecase pgen
            pmin = net.gen.loc[gidx, 'min_p_mw']                                                    # this generators pmin
            pmax = net.gen.loc[gidx, 'max_p_mw']                                                    # this generators max
            pfactor = pfactor_dict[gen_pkey]                                                        # this generators participation factor
            target_pgen = pgen_a + pfactor * delta                                                  # calculate this generators expected pgen
            if pmin < target_pgen < pmax:                                                           # if expected pgen is in bounds...
                net.gen.loc[gidx, 'p_mw'] = target_pgen                                             # set pgen = expected pgen
            elif target_pgen > pmax:                                                                # if expected pgen > pmax...
                net.gen.loc[gidx, 'p_mw'] = pmax                                                    # set pgen = pmax
            elif target_pgen < pmin:                                                                # if expected pgen < pmin...
                net.gen.loc[gidx, 'p_mw'] = pmin                                                    # set pgen = pmin
        pp.runpp(net, enforce_q_lims=True)                                                          # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # get external grid power
        if abs(ex_pgen) < 1e-3:                                                                     # if external grid power is near zero..
            break                                                                                   # break and get next gen outage
        delta += ex_pgen / pfactor_total                                                            # increment delta
        step += 1                                                                                   # increment iteration

    line_loading = max(net.res_line['loading_percent'].values)                                      # get max line loading
    xfmr_loading = max(net.res_trafo['loading_percent'].values)                                     # get max line loading
    max_loading = max(line_loading, xfmr_loading)                                                   # get max of maxs
    ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                            # get external grid real power
    ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                          # get external grid reactive power
    print('GEN {0:6s} . . . . . . . . . . . . . . . . . . . . . . . . . . . . .'.format(genkey),
          '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6), round(max_loading, 1))

    if step == 120:
        time.sleep(1)
        net.gen.loc[swinggen_idxs, 'slack'] = True
        pp.runpp(net, enforce_q_lims=True)                                                          # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # get external grid power
        # print(ex_pgen)
        # print(net.res_gen.loc[swinggen_idxs, 'p_mw'])
        print(genkey, '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', 'SOMETHING GOING ON', participating_units, ex_pgen, net.res_gen.loc[swinggen_idxs, 'p_mw'])

        time.sleep(1)

    return [conlabel, net.res_bus, net.res_gen, delta]


def run_branch_outages(branchkey, c_net, a_net, base_pgens, line_dict, xfmr_dict, outage_dict, swsh_dict, swshbus_dict, area_participating_gens,
                       pfactor_dict, gen_dict, branch_areas, ext_grid_idx, Multiprocessing):
    """ run outaged branches and calculate delta variable """
    btext = ''                                                                                      # declare branch type text
    bnet = copy.deepcopy(c_net)                                                                     # initialize this contingency network
    conlabel = outage_dict['branch'][branchkey]                                                     # get contingency label

    participating_units = []                                                                        # initialize participating generator list
    outage_areas = branch_areas[branchkey]                                                          # get areas connected to branch
    for outage_area in outage_areas:                                                                # loop across branch areas
        participating_units += area_participating_gens[outage_area]                                 # add area generators to participating generator list

    pfactor_total = 0.0                                                                             # initialize participation factors total
    for gen_pkey in participating_units:                                                            # loop across participating generators
        pfactor_total += pfactor_dict[gen_pkey]                                                     # increment pfactor total

    if branchkey in line_dict:                                                                      # check if branch is a line...
        btext = 'LINE'                                                                              # assign branch type text
        lineidx = line_dict[branchkey]                                                              # get line index
        bnet.line.in_service[lineidx] = False                                                       # take line out of service
    elif branchkey in xfmr_dict:                                                                    # check if branch is a xfmr...
        btext = 'XFMR'                                                                              # assign branch type text
        xfmridx = xfmr_dict[branchkey]                                                              # get xfmr index
        bnet.trafo.in_service[xfmridx] = False                                                      # take xfmr out of service


    try:                                                                                            # try straight power flow
        pp.runpp(bnet, enforce_q_lims=True)                                                         # run straight power flow
    except:                                                                                         # if not solved...
        for gkey in participating_units:                                                            # loop across participating generators
            gidx = gen_dict[gkey]                                                                   # get generator index
            bnet.gen.loc[gidx, 'max_q_mvar'] = 999.9                                                # set qmax very large positive
            # bnet.gen.loc[gidx, 'min_q_mvar'] = -999.9                                               # set qmin very large negative
        try:                                                                                        # try straight power flow again...
            pp.runpp(bnet, enforce_q_lims=True)                                                    # run straight power flow
        except:                                                                                     # if not solved... write basecase results as placeholder
            print('{0:4s} {1:11s} DID NOT SOLVE WITH POWERFLOW AND QMAX=999 .........'.format(btext, branchkey))
            delta = 0.0                                                                             # assign delta = 0
            _bus_results = copy.deepcopy(bnet.res_bus)                                              # get basecase bus results
            _gen_results = copy.deepcopy(bnet.res_gen)                                              # get basecase generator results
            return conlabel, _bus_results, _gen_results, delta                                      # return 'dummy' basecase results placeholder

    # -- INSURE SWSHUNTS SUSCEPTANCE IS WITHIN LIMITS -------------------------
    for shkey in swsh_dict:                                                                         # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_dict[shkey]                                                                    # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        qgen = bnet.res_gen.loc[shidx, 'q_mvar']
        qmin = bnet.gen.loc[shidx, 'min_q_mvar']                                                     # GET MINIMUM SWSHUNT REACTIVE CAPABILITY
        qmax = bnet.gen.loc[shidx, 'max_q_mvar']                                                     # GET MAXIMUM SWSHUNT REACTIVE CAPABILITY
        voltage = bnet.res_bus.loc[shbus, 'vm_pu']                                                   # GET SWSHUNT BUS VOLTAGE
        if voltage < 1.0:                                                                           # IF BUS VOLTAGE IS < 1.0 (SUSCEPTANCE COULD BE EXCEEDED)
            if qgen / voltage ** 2 < 0.98 * qmin < 0.0:                                             # CHECK IF QMIN IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmin = min(qmax, 0.99 * qmin * voltage ** 2)                                    # CALCULATE QMIN THAT IS IN BOUNDS
                bnet.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                         # ADJUST QMIN IN POSITIVE DIRECTION WITH SOME EXTRA
                print(shkey, 'Adj QMIN Up', 'QMIN =', qmin, 'NEW QMIN =', new_qmin)
                pp.runpp(bnet, enforce_q_lims=True)                                                  # THIS NETWORK, RUN STRAIGHT POWER FLOW
            elif qgen / voltage ** 2 > 0.98 * qmax > 0.0:                                           # CHECK IF QMAX IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmax = max(qmin, 0.99 * qmax * voltage ** 2)                                    # CALCULATE QMAX THAT IS IN BOUNDS
                bnet.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                         # ADJUST QMAX IN NEGATIVE DIRECTION WITH SOME EXTRA
                print(shkey, 'Adj QMAX Down', 'QMAX =', qmax, 'NEW QMAX =', new_qmax)
                pp.runpp(bnet, enforce_q_lims=True)                                                  # THIS NETWORK, RUN STRAIGHT POWER FLOW

    # -- ITERATE TO FIND OPTIMUM BRANCH OUTAGE DELTA VARIABLE  ---------------------------------
    step = 1                                                                                        # initialize iterator
    delta = 0.0                                                                                     # initialize delta variable
    net = copy.deepcopy(bnet)                                                                       # get fresh copy initialized network
    while step < 120:                                                                               # limit while loops
        for gen_pkey in participating_units:                                                        # loop through participating generators
            gidx = gen_dict[gen_pkey]                                                               # get this generator index
            pgen_a = base_pgens[gen_pkey]                                                           # this generators ratea basecase pgen
            pmin = net.gen.loc[gidx, 'min_p_mw']                                                    # this generators pmin
            pmax = net.gen.loc[gidx, 'max_p_mw']                                                    # this generators max
            pfactor = pfactor_dict[gen_pkey]                                                        # this generators participation factor
            target_pgen = pgen_a + pfactor * delta                                                  # calculate this generators expected pgen
            if pmin < target_pgen < pmax:                                                           # if expected pgen is in bounds...
                net.gen.loc[gidx, 'p_mw'] = target_pgen                                             # set pgen = expected pgen
            elif target_pgen > pmax:                                                                # if expected pgen > pmax...
                net.gen.loc[gidx, 'p_mw'] = pmax                                                    # set pgen = pmax
            elif target_pgen < pmin:                                                                # if expected pgen < pmin...
                net.gen.loc[gidx, 'p_mw'] = pmin                                                    # set pgen = pmin
        pp.runpp(net, enforce_q_lims=True)                                                          # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # get external grid power
        if abs(ex_pgen) < 1e-3:                                                                     # if external grid power is near zero..
            break                                                                                   # break and get next gen outage
        delta += ex_pgen / pfactor_total                                                            # increment delta
        step += 1                                                                                   # increment iteration
    line_loading = max(net.res_line['loading_percent'].values)                                      # get max line loading
    xfmr_loading = max(net.res_trafo['loading_percent'].values)                                     # get max line loading
    max_loading = max(line_loading, xfmr_loading)                                                   # get max of maxs
    ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                            # get external grid real power
    ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                          # get external grid reactive power
    print('{0:4s} {1:11s} . . . . . . . . . . . . . . . . . . . . . . . . . .'.format(btext, branchkey),
          '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6), round(max_loading, 1))

    if step == 120:
        for gkey in participating_units:
            gidx = gen_dict[gkey]
            print(branchkey, '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', participating_units, net.res_gen.loc[gidx, 'p_mw'], net.gen.loc[gidx, 'max_p_mw'])
            # print(net.res_gen.loc[gidx, 'p_mw'], net.gen.loc[gidx, 'max_p_mw'])
    return [conlabel, net.res_bus, net.res_gen, delta]


def arghelper1(args):
    return run_gen_outages(*args)


def parallel_run_gen_outages(gen_arglist):
    pool = multiprocessing.Pool(processes=int(os.environ['NUMBER_OF_PROCESSORS']))
    g_results = pool.map_async(arghelper1, gen_arglist)
    pool.close()
    pool.join()
    return g_results.get()


def arghelper2(args):
    return run_branch_outages(*args)


def parallel_run_branch_outages(branch_arglist):
    pool = multiprocessing.Pool(processes=int(os.environ['NUMBER_OF_PROCESSORS']))
    b_results = pool.map_async(arghelper2, branch_arglist)
    pool.close()
    pool.join()
    return b_results.get()


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 1000
    pdoptions.display.max_rows = 1000
    pdoptions.display.max_colwidth = 199
    pdoptions.display.width = None
    pdoptions.display.precision = 4
    # print()
    # print('BUS DATAFRAME')
    # print(_net.bus)
    # print()
    # print('BUS RESULTS')
    # print(_net.res_bus)
    # print()
    # print('LOAD DATAFRAME')
    # print(_net.load)
    # print()
    # print('FXSHUNT DATAFRAME')
    # print(_net.shunt)
    # print()
    # print('FXSHUNT RESULTS')
    # print(_net.res_shunt)
    # print()
    # print('LINE DATAFRAME')
    # print(_net.line)
    # print()
    # print('LINE RESULTS')
    # print(_net.res_line)
    # print('MAX LINE LOADING % =', max(_net.res_line['loading_percent'].values))
    # print()
    # print('TRANSFORMER DATAFRAME')
    # print(_net.trafo)
    # print()
    # print('TRANSFORMER RESULTS')
    # print(_net.res_trafo)
    # print('MAX XFMR LOADING % =', max(_net.res_trafo['loading_percent'].values))
    print()
    print('GENERATOR DATAFRAME')
    print(_net.gen)
    print()
    print('GENERATOR RESULTS')
    print(_net.res_gen)
    print()
    # print('EXT GRID DATAFRAME')
    # print(_net.ext_grid)
    # print()
    # print('EXT GRID RESULTS')
    # print(_net.res_ext_grid)
    # print()
    return

def get_branch_losses(line_res,  trafo_res):
    losses = 0.0
    pfrom = line_res['p_from_kw'].values
    pto = line_res['p_to_kw'].values
    line_losses = numpy.add(pfrom, pto)
    line_losses = [abs(x) for x in line_losses]
    line_losses = sum(line_losses)
    losses += line_losses
    pfrom = trafo_res['p_hv_kw'].values
    pto = trafo_res['p_lv_kw'].values
    xfmr_losses = numpy.add(pfrom, pto)
    xfmr_losses = [abs(x) for x in xfmr_losses]
    xfmr_losses = sum(xfmr_losses)
    losses += xfmr_losses
    return losses


# =================================================================================================
# -- MYPYTHON_2 -----------------------------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    print()
    cwd = os.getcwd()
    start_time = time.time()

    # =============================================================================================
    # -- GET DATA FROM FILES ----------------------------------------------------------------------
    # =============================================================================================
    print('---------------------- GETTING DATA FROM FILE ----------------------')
    try:
        net_a = pp.from_pickle(neta_fname)
    except FileNotFoundError:
        raise Exception('COULD NOT FIND THIS FILE -->', neta_fname)
    try:
        net_c = pp.from_pickle(netc_fname)
    except FileNotFoundError:
        print('COULD NOT FIND THIS FILE -->', netc_fname)
    try:
        PFile = open(data_fname, 'rb')
        Base_pgens, Ext_grid_idx, Genbuses, Genbus_dict, Gids, Line_dict, Goutagekeys, Boutagekeys, Outage_dict, Gen_dict,\
            Pfactor_dict, Xfmr_dict, Area_swhunts, Swsh_dict, Swshbus_dict, Swshidxs, Swshidx_dict, Busarea_dict, Branch_areas, \
            Area_participating_gens, Swinggen_idxs, Alt_sw_genkey, Tie_idx, Alt_tie_idx = pickle.load(PFile)
        PFile.close()
    except FileNotFoundError:
        raise Exception('COULD NOT FIND THIS FILE -->', data_fname)


    # TODO testing multiprocessing ---------------------------
    Multiprocessing = True

    if not Multiprocessing:
        # =========================================================================================
        # -- RUN GENERATOR OUTAGES ----------------------------------------------------------------
        # =========================================================================================
        print('-------------------- RUNNING GENERATOR OUTAGES ---------------------')
        gopf_starttime = time.time()
        fobject = open(outfname2, 'w', newline='')
        Writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for Genkey in Goutagekeys:
            Conlabel, Bus_results, Gen_results, Delta = run_gen_outages(Genkey, net_c, net_a, Base_pgens, Outage_dict, Area_participating_gens,
                                                                        Pfactor_dict, Gen_dict, Genbus_dict, Busarea_dict, Ext_grid_idx,
                                                                        Swinggen_idxs, Alt_sw_genkey, Tie_idx, Alt_tie_idx, Multiprocessing)

            # == WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ==============================
            Conlabel = "'" + Conlabel + "'"
            write_bus_results(outfname2, Bus_results, Swshidx_dict, Gen_results, Ext_grid_idx, Conlabel, Writer)
            write_gen_results(outfname2, Gen_results, Gids, Genbuses, Delta, Swshidxs, Writer)
        print('GENERATOR OUTAGES FINISHED .........................................', round(time.time() - gopf_starttime, 1))

        # =========================================================================================
        # -- RUN LINE AND TRANSFORMER OUTAGES -----------------------------------------------------
        # =========================================================================================
        print('---------------------- RUNNING BRANCH OUTAGES ----------------------')
        bopf_starttime = time.time()
        for Branchkey in Boutagekeys:
            Conlabel, bus_results, gen_results, Delta = run_branch_outages(Branchkey, net_c, net_a, Base_pgens, Line_dict, Xfmr_dict, Outage_dict, Swsh_dict, Swshbus_dict,
                                                                           Area_participating_gens, Pfactor_dict, Gen_dict,  Branch_areas, Ext_grid_idx, Multiprocessing)
            # == WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ==============================
            Conlabel = "'" + Conlabel + "'"
            write_bus_results(outfname2, bus_results, Swshidx_dict, gen_results, Ext_grid_idx, Conlabel, Writer)
            write_gen_results(outfname2, gen_results, Gids, Genbuses, Delta, Swshidxs, Writer)
        fobject.close()
        print('LINE AND TRANSFORMER OUTAGES FINISHED ..............................', round(time.time() - bopf_starttime, 3))

    if Multiprocessing:
        gen_arglist = [[genkey, net_c, net_a, Base_pgens, Outage_dict, Area_participating_gens,
                        Pfactor_dict, Gen_dict, Genbus_dict, Busarea_dict, Ext_grid_idx, Swinggen_idxs,
                        Alt_sw_genkey, Tie_idx, Alt_tie_idx, Multiprocessing] for genkey in Goutagekeys]
        gresults = parallel_run_gen_outages(gen_arglist)
        branch_arglist = [[branchkey, net_c, net_a, Base_pgens, Line_dict, Xfmr_dict, Outage_dict, Swsh_dict, Swshbus_dict,
                           Area_participating_gens, Pfactor_dict, Gen_dict,  Branch_areas, Ext_grid_idx, Multiprocessing] for branchkey in Boutagekeys]
        bresults = parallel_run_branch_outages(branch_arglist)

        # == WRITE CONTINGENCY GENERATOR AND BRANCH RESULTS TO FILE ===============================
        print('WRITING GENERATORS AND BRANCHES SOLUTION2 FILE .....................')
        fobject = open(outfname2, 'w', newline='')
        Writer = csv.writer(fobject, delimiter=',', quotechar='"')
        all_results = gresults + bresults
        for result in all_results:
            Conlabel, Bus_results, Gen_results, Delta = result
            Conlabel = "'" + Conlabel + "'"                                                             # FORMAT CONLABEL FOR OUTPUT
            write_bus_results(outfname2, Bus_results, Swshidx_dict, Gen_results, Ext_grid_idx, Conlabel, Writer)
            write_gen_results(outfname2, Gen_results, Gids, Genbuses, Delta, Swshidxs, Writer)
        fobject.close()

    print('=========================== DONE ===================================')
    print()
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))
