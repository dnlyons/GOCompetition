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
cwd = os.path.dirname(__file__)

# -- DEVELOPMENT DEFAULT ------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/case.con'
    inl_fname = cwd + r'/sandbox/Network_01-10O/case.inl'
    raw_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/case.raw'
    rop_fname = cwd + r'/sandbox/Network_01-10O/case.rop'
    outfname1 = cwd + r'/sandbox/Network_01-10O/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/Network_01-10O/scenario_1/solution2.txt'

# -- USING COMMAND LINE -------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    outfname1 = 'solution1.txt'
    outfname2 = 'solution2.txt'

try:
    os.remove(outfname2)
except FileNotFoundError:
    pass


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def listoflists(tt):
    """ convert tuple_of_tuples to list_of_lists """
    return list((listoflists(x) if isinstance(x, tuple) else x for x in tt))


def tupleoftuples(ll):
    """ convert list_of_lists to tuple_of_tuples """
    return tuple((tupleoftuples(x) if isinstance(x, list) else x for x in ll))


def write_csvdata(fname, lol, label):
    with open(fname, 'a', newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus, clabel):
    # -- DELETE UNUSED DATAFRAME COLUMNS --------------------------------------
    try:
        del b_results['p_kw']        # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['q_kvar']      # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['lam_p']       # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['lam_q']       # not used for reporting
    except KeyError:
        pass
    # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
    b_results.drop([exgridbus], inplace=True)
    # -- ADD BUSNUMBER COLUMN -------------------------------------------------
    b_results.insert(0, 'bus', b_results.index)
    # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
    b_results['sh_mvars'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage_pu', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage_pu'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    # -- (SWITCHED SHUNTS ARE MODELED AS GENERATORS) --------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus = buslist[j][0]
        mvars = 0.0
        if bus in sw_dict:
            mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar'] / (b_results.loc[bus, 'voltage_pu'] ** 2)
            # mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar']
            buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_gen_results(fname, g_results, genids, gbuses, delta, ssh_idxs):
    g_results.drop(ssh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- CONVERT BACK TO MW AND MVARS -----------------------------------------
    g_results['p_kw'] *= -1e-3
    g_results['q_kvar'] *= -1e-3
    g_results['p_kw'] += 0.0
    g_results['q_kvar'] += 0.0
    delta *= -1e-3
    # pgen_out *= -1e-3
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    g_results.rename(columns={'p_kw': 'mw', 'q_kvar': 'mvar'}, inplace=True)
    # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
    g_results.insert(0, 'id', genids)
    g_results.insert(0, 'bus', gbuses)    # -- CALCULATE TOTAL POWER OF PARTICIPATING GENERATORS --------------------
    # c_gens = sum([x for x in g_results['mw'].values])
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    # deltapgens = p_delta + pgen_out
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [delta]])
    return


def run_gen_outage_get_delta(genkey, c_net, a_net, outage_dict, pfactor_dict, gendict, swshidx_dict):
    """ run outaged generators and calculate delta variable """
    gnet = copy.deepcopy(c_net)                                                                     # get fresh copy of ratec network
    anet = copy.deepcopy(a_net)                                                                     # get fresh copy of ratec network
    conlabel = outage_dict['gen'][genkey]                                                          # get contingency label
    genidx = gendict[genkey]                                                                       # get outaged generator index
    pgen_outage_ = anet.gen.loc[genidx, 'p_kw']                                                    # get outaged generator pgen
    gnet.gen.in_service[genidx] = False                                                            # switch off outaged generator

    # -- CALCULATE PARTICIPATING GENERATORS UP MARGIN ---------------------------------------------
    p_margin_total = 0.0                                                                           # initialize gens total reserve
    p_margin_dict = {}                                                                             # initialize gen reserve dict
    for gen_pkey in pfactor_dict:                                                                  # loop through participating generators
        gidx = gendict[gen_pkey]                                                                  # get this participating generator index
        if not gnet.gen.loc[gidx, 'in_service']:                                                   # check if participating generator is online...
            continue                                                                                # if not... get next participating generator
        pgen = gnet.gen.loc[gidx, 'p_kw']                                                          # this generators ratec basecase pgen
        pmin = gnet.gen.loc[gidx, 'min_p_kw']                                                      # this generators ratec basecase pmin
        margin = pmin - pgen                                                                        # this generators up reserve margin
        p_margin_dict.update({gidx: margin})                                                      # update dict... this generators up reserve margin
        p_margin_total += margin                                                                   # increment total up reserve margin

    # -- FIRST ESTIMATE OF DELTA VARIABLE ---------------------------------------------------------
    pfactor_total = 0.0                                                                            # initialize total of all participation factors
    for gen_pkey in pfactor_dict:                                                                  # loop through participating generators
        gidx = gendict[gen_pkey]                                                                  # get this participating generator index
        if not gnet.gen.loc[gidx, 'in_service']:                                                   # check if participating generator is online...
            continue                                                                                # if not... get next participating generator
        pfactor_total += pfactor_dict[gen_pkey]                                                   # increment pfactor total
        pgen_a = anet.gen.loc[gidx, 'p_kw']                                                        # this generators ratea basecase pgen
        delta_pgen = pgen_outage_ * p_margin_dict[gidx] / p_margin_total                         # calculate this generators change (proportional to up margin)
        gnet.gen.loc[gidx, 'p_kw'] = pgen_a + delta_pgen                                           # set this generators pgen
    pp.runpp(gnet, enforce_q_lims=True)                                                             # run straight power flow
    ex_pgen = gnet.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                           # get external grid power
    delta_init = (pgen_outage_ + ex_pgen) / pfactor_total                                          # first estimate of delta variable

    pp.runopp(gnet, enforce_q_lims=True)                                                            # run optimal power flow (optimize swshunts)
    swsh_q_mins = gnet.gen.loc[swshidxs, 'min_q_kvar']                                              # get copy of swshunt qmins
    swsh_q_maxs = gnet.gen.loc[swshidxs, 'max_q_kvar']                                              # get copy of swshunt qmaxs
    for shbus in swshidx_dict:                                                                       # loop across swshunt (gen) buses
        shidx = swshidx_dict[shbus]                                                                  # get swshunt index
        busv = gnet.res_bus.loc[shbus, 'vm_pu']                                                     # get opf swshunt bus voltage
        kvar = gnet.res_gen.loc[shidx, 'q_kvar']                                                    # get opf vars of swshunt
        kvar_1pu = kvar / busv ** 2                                                                 # calculate vars susceptance
        if busv > 1.0:                                                                              # if bus voltage > 1.0 pu...
            gnet.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                            # set min swshunt vars
        elif busv < 1.0:                                                                            # if bus voltage < 1.0 pu...
            gnet.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                            # set max swshunt vars
    pp.runpp(gnet, enforce_q_lims=True)                                                             # run straight power flow
    for shbus in swshidx_dict:                                                                       # loop across swshunt (gen) buses
        shidx = swshidx_dict[shbus]                                                                  # get swshunt index
        gnet.gen.loc[shidx, 'vm_pu'] = gnet.res_bus.loc[shbus, 'vm_pu']                             # set swshunt vreg to opf swshunt bus voltage
    pp.runpp(gnet, enforce_q_lims=True)                                                             # run straight power flow
    gnet.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                              # restore original swshunt qmins
    gnet.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                              # restore original swshunt qmaxs
    # print_dataframes_results(gnet)

    # -- ITERATE TO FIND OPTIMUM DELTA VARIABLE  --------------------------------------------------
    step = 1
    delta = delta_init
    while step < 120:                                                                               # limit while loops
        net = copy.deepcopy(gnet)                                                                  # get fresh copy initialized network
        # net.gen.in_service[genidx] = False                                                        # switch off outaged generator
        for gen_pkey in pfactor_dict:                                                              # loop through participating generators
            gidx = gendict[gen_pkey]                                                              # get this participating generator index
            if not net.gen.loc[gidx, 'in_service']:                                               # check if participating generator is online...
                continue                                                                            # if not... get next participating generator
            pgen_a = anet.gen.loc[gidx, 'p_kw']                                                    # this generators ratea basecase pgen
            pmin = net.gen.loc[gidx, 'min_p_kw']                                                  # this generators pmin
            pmax = net.gen.loc[gidx, 'max_p_kw']                                                  # this generators max
            pfactor = pfactor_dict[gen_pkey]                                                       # this generators participation factor
            target_pgen = pgen_a + pfactor * delta                                                 # calculate this generators expected pgen
            if pmin < target_pgen < pmax:                                                           # if expected pgen is in bounds...
                net.gen.loc[gidx, 'p_kw'] = target_pgen                                           # set pgen = expected pgen
            elif target_pgen < pmin:                                                                # if expected pgen < pmin...
                net.gen.loc[gidx, 'p_kw'] = pmin                                                  # set pgen = pmin
            elif target_pgen > pmax:                                                                # if expected pgen > pmax...
                net.gen.loc[gidx, 'p_kw'] = pmax                                                  # set pgen = pmax
        # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS ----
        for shbus in swshidx_dict:                                                                  # loop across swshunt (gen) buses
            shidx = swshidx_dict[shbus]                                                             # get swshunt index
            busv = net.res_bus.loc[shbus, 'vm_pu']                                                 # get opf swshunt bus voltage
            kvar = net.res_gen.loc[shidx, 'q_kvar']                                                # get opf vars of swshunt
            kvar_1pu = kvar / busv ** 2                                                             # calculate vars susceptance
            if busv > 1.0:                                                                          # if bus voltage > 1.0 pu...
                net.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                       # set min swshunt vars
            if busv < 1.0:                                                                          # if bus voltage < 1.0 pu...
                net.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                       # set max swshunt vars
        pp.runpp(net, enforce_q_lims=True)                                                    # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                       # get external grid power
        delta += ex_pgen / pfactor_total                                                          # increment delta
        if abs(ex_pgen) < 1.0:                                                                     # check if external grid is near zero...
            break                                                                                   # delta is close enough... break and return results
        step += 1                                                                                   # increment iteration
    # print_dataframes_results(net)
    ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']
    print('GEN {0:5s} ..........................................................'.format(genkey),
          '\u0394 =', round(-1e-3 * delta, 5), '(' + str(step) + ')', round(ex_pgen, 3), round(ex_qgen + 0.0, 6))
    return conlabel, net.res_bus, net.res_gen, delta


def run_branch_outage_get_delta(branchkey, c_net, a_net, line_dict, xfmr_dict, outage_dict, pfactor_dict, gen_dict, swshidx_dict, pfactor_total, swinggen_idxs):
    """ run outaged branches and calculate delta variable """
    bnet = copy.deepcopy(c_net)                                                                     # get fresh copy of ratec network
    anet = copy.deepcopy(a_net)                                                                     # get fresh copy of ratea network
    conlabel = outage_dict['branch'][branchkey]                                                     # get contingency label
    swsh_q_mins = bnet.gen.loc[swshidxs, 'min_q_kvar']                                              # get copy of swshunt qmins
    swsh_q_maxs = bnet.gen.loc[swshidxs, 'max_q_kvar']                                              # get copy of swshunt qmaxs
    if branchkey in line_dict:                                                                      # check if branch is a line...
        lineidx = line_dict[branchkey]                                                              # get line index
        bnet.line.in_service[lineidx] = False                                                       # take line out of service
    elif branchkey in xfmrdict:                                                                     # check if branch is a xfmr...
        xfmridx = xfmr_dict[branchkey]                                                              # get xfmr index
        bnet.trafo.in_service[xfmridx] = False                                                      # take xfmr out of service
    try:
        pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    except:
        print('BRANCH {0:9s} ...................................................'.format(branchkey), 'NO SOLUTION ---> INITIALIZE WITH OPF')
        return None, None, None, None

    for shbus in swshidx_dict:                                                                      # loop across swshunt (gen) buses
        shidx = swshidx_dict[shbus]                                                                 # get swshunt index
        busv = bnet.res_bus.loc[shbus, 'vm_pu']                                                     # get opf swshunt bus voltage
        kvar = bnet.res_gen.loc[shidx, 'q_kvar']                                                    # get opf vars of swshunt
        kvar_1pu = kvar / busv ** 2                                                                 # calculate vars susceptance
        if busv > 1.0:                                                                              # if bus voltage > 1.0 pu...
            bnet.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                            # set min swshunt vars
        elif busv < 1.0:                                                                            # if bus voltage < 1.0 pu...
            bnet.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                            # set max swshunt vars
    try:
        pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    except:
        print('BRANCH {0:9s} ...................................................'.format(branchkey), 'NO SOLUTION ---> INITIALIZE WITH OPF')
        return None, None, None, None

    for shbus in swshidx_dict:                                                                      # loop across swshunt (gen) buses
        shidx = swshidx_dict[shbus]                                                                 # get swshunt index
        bnet.gen.loc[shidx, 'vm_pu'] = bnet.res_bus.loc[shbus, 'vm_pu']                             # set swshunt vreg to opf swshunt bus voltage

    try:
        pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    except:
        print('BRANCH {0:9s} ...................................................'.format(branchkey), 'NO SOLUTION ---> INITIALIZE WITH OPF')
        return None, None, None, None

    bnet.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                              # restore original swshunt qmins
    bnet.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                              # restore original swshunt qmaxs

    # -- ITERATE TO FIND OPTIMUM BRANCH OUTAGE DELTA VARIABLE  ---------------------------------
    delta = 0.0
    step = 1
    net = copy.deepcopy(bnet)  # get fresh copy initialized network
    while step < 120:                                                                               # limit while loops
        for gen_pkey in pfactor_dict:                                                               # loop through participating generators
            gidx = gen_dict[gen_pkey]                                                               # get this generator index
            if not net.gen.loc[gidx, 'in_service']:                                                 # check if generator is online
                continue
            pgen_a = anet.res_gen.loc[gidx, 'p_kw']                                                 # this generators ratea basecase pgen
            pmin = net.gen.loc[gidx, 'min_p_kw']                                                    # this generators pmin
            pmax = net.gen.loc[gidx, 'max_p_kw']                                                    # this generators max
            pfactor = pfactor_dict[gen_pkey]                                                        # this generators participation factor
            target_pgen = pgen_a + pfactor * delta                                                  # calculate this generators expected pgen
            if pmin < target_pgen < pmax:                                                           # if expected pgen is in bounds...
                net.gen.loc[gidx, 'p_kw'] = target_pgen                                             # set pgen = expected pgen
            elif target_pgen < pmin:                                                                # if expected pgen < pmin...
                net.gen.loc[gidx, 'p_kw'] = pmin                                                    # set pgen = pmin
            elif target_pgen > pmax:                                                                # if expected pgen > pmax...
                net.gen.loc[gidx, 'p_kw'] = pmax                                                    # set pgen = pmax
        # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS ----
        for shbus in swshidx_dict:                                                                  # loop across swshunt (gen) buses
            shidx = swshidx_dict[shbus]                                                             # get swshunt index
            busv = net.res_bus.loc[shbus, 'vm_pu']                                                  # get opf swshunt bus voltage
            kvar = net.res_gen.loc[shidx, 'q_kvar']                                                 # get opf vars of swshunt
            kvar_1pu = kvar / busv ** 2                                                             # calculate vars susceptance
            if busv > 1.0:                                                                          # if bus voltage > 1.0 pu...
                net.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                         # set min swshunt vars
            elif busv < 1.0:                                                                        # if bus voltage < 1.0 pu...
                net.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                         # set max swshunt vars
        pp.runpp(net, enforce_q_lims=True)                                                          # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                        # get external grid power
        if abs(ex_pgen) < 1.0:                                                                      # check if external grid is near zero...
            break                                                                                   # delta is close enough... break and return results
        delta += ex_pgen / pfactor_total                                                            # increment delta
        step += 1                                                                                   # increment iteration
        net.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                           # restore original swshunt qmins
        net.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                           # restore original swshunt qmaxs

    for idx in swinggen_idxs:                                                                       # loop across generators connected to swing bus
        net.gen.loc[idx, 'p_kw'] += ex_pgen / len(swinggen_idxs)                                    # distribute ex_pgen across swing generators
    pp.runpp(net, enforce_q_lims=True)                                                              # run final straight power flow on contingency
    ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                            # get external grid real power
    ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']                                          # get external grid reactive power
    print('BRANCH {0:9s} ...................................................'.format(branchkey),    # print statement
          '\u0394 =', round(-1e-3 * delta, 5), '(' + str(step) + ')', round(ex_pgen, 3), round(ex_qgen + 0.0, 6))
    return conlabel, net.res_bus, net.res_gen, delta


def run_robust_branch_outage_get_delta(branchkey, c_net, a_net, line_dict, xfmr_dict, outage_dict, pfactor_dict, gen_dict, swshidx_dict, pfactor_total, genidx_dict, swinggen_idxs):
    """ run outaged branches and calculate delta variable """
    bnet = copy.deepcopy(c_net)                                                                     # get fresh copy of ratec network
    anet = copy.deepcopy(a_net)                                                                     # get fresh copy of ratea network
    conlabel = outage_dict['branch'][branchkey]                                                     # get contingency label
    swsh_q_mins = bnet.gen.loc[swshidxs, 'min_q_kvar']                                              # get copy of swshunt qmins
    swsh_q_maxs = bnet.gen.loc[swshidxs, 'max_q_kvar']                                              # get copy of swshunt qmaxs

    net = copy.deepcopy(c_net)                                                                      # get fresh copy of ratec network
    for genbus in genidx_dict:                                                                      # loop across generator buses
        gidx = genidx_dict[genbus]                                                                  # get generator index
        if not net.gen.loc[gidx, 'in_service']:                                                     # check if in-service
            continue
        net.gen.loc[gidx, 'controllable'] = False                                                   # turn off generator opf control

    if branchkey in line_dict:                                                                      # check if branch is a line...
        lineidx = line_dict[branchkey]                                                              # get line index
        net.line.in_service[lineidx] = False                                                        # take line out of service
    elif branchkey in xfmr_dict:                                                                    # check if branch is a xfmr...
        xfmridx = xfmr_dict[branchkey]                                                              # get xfmr index
        net.trafo.in_service[xfmridx] = False                                                       # take xfmr out of service

    pp.runopp(net, enforce_q_lims=True)                                                             # run OPF on contingency
    for shbus in swshidx_dict:                                                                      # loop across swshunt (gen) buses
        shidx = swshidx_dict[shbus]                                                                 # get swshunt index
        bnet.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                              # set swshunt vreg to opf swshunt bus voltage
        # print(shbus, net.res_bus.loc[shbus, 'vm_pu'])

    solved = False                                                                                  # initialize solved flag
    step = 1                                                                                        # initialize iteration step
    delta = 0.0                                                                                     # initialize delta variable
    net = copy.deepcopy(bnet)
    while step < 120:                                                                               # limit iterations
        # net = copy.deepcopy(bnet)
        for gen_pkey in pfactor_dict:                                                               # loop through participating generators
            gidx = gen_dict[gen_pkey]                                                               # get this generator index
            if not net.gen.loc[gidx, 'in_service']:                                                 # check if generator is online
                continue
            pgen_a = anet.res_gen.loc[gidx, 'p_kw']                                                 # this generators ratea basecase pgen
            pmin = net.gen.loc[gidx, 'min_p_kw']                                                    # this generators pmin
            pmax = net.gen.loc[gidx, 'max_p_kw']                                                    # this generators pmax
            pfactor = pfactor_dict[gen_pkey]                                                        # this generators participation factor
            target_pgen = pgen_a + pfactor * delta                                                  # calculate this generators expected pgen
            if pmin < target_pgen < pmax:                                                           # if expected pgen is in bounds...
                net.gen.loc[gidx, 'p_kw'] = target_pgen                                             # set pgen = expected pgen
            elif target_pgen < pmin:                                                                # if expected pgen < pmin...
                net.gen.loc[gidx, 'p_kw'] = pmin                                                    # set pgen = pmin
            elif target_pgen > pmax:                                                                # if expected pgen > pmax...
                net.gen.loc[gidx, 'p_kw'] = pmax                                                    # set pgen = pmax
        try:
            pp.runpp(net, enforce_q_lims=True)                                                      # run straight power flow on contingency
            # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS ----
            for shbus in swshidx_dict:                                                              # loop across swshunt (gen) buses
                shidx = swshidx_dict[shbus]                                                         # get swshunt index
                busv = net.res_bus.loc[shbus, 'vm_pu']                                              # get opf swshunt bus voltage
                kvar = net.res_gen.loc[shidx, 'q_kvar']                                             # get opf vars of swshunt
                kvar_1pu = kvar / busv ** 2                                                         # calculate vars susceptance
                if busv > 1.0:                                                                      # if bus voltage > 1.0 pu...
                    net.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                     # set min swshunt vars
                elif busv < 1.0:                                                                    # if bus voltage < 1.0 pu...
                    net.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                     # set max swshunt vars
                net.gen.loc[shidx, 'vm_pu'] = busv                                                  # set swshunt vreg to opf swshunt bus voltage
            pp.runpp(net, enforce_q_lims=True)                                                      # run final straight power flow on basecase
            net.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins  # restore original swshunt qmins
            net.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs  # restore original swshunt qmaxs
            pp.runpp(net, enforce_q_lims=True)                                                      # run final straight power flow on basecase
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                    # get external grid power
            ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']

            if abs(ex_pgen) < 1.0:                                                                  # check if external grid is near zero...
                # for genbus in genidx_dict:  # LOOP ACROSS GENERATOR BUSES
                #     gidx = genidx_dict[genbus]  # GET GENERATOR INDEX
                #     print(genbus, net.gen.loc[gidx, 'vm_pu'], net.res_bus.loc[genbus, 'vm_pu'], net.res_gen.loc[gidx, 'q_kvar'], net.gen.loc[gidx, 'min_q_kvar'], net.gen.loc[gidx, 'max_q_kvar'] )
                # print_dataframes_results(net)

                solved = True                                                                       # set solved flag
                bus_results = net.res_bus
                gen_results = net.res_gen
                best_delta = delta
                best_ex_pgen = ex_pgen
                best_ex_qgen = ex_qgen
                best_step = step
                break                                                                               # delta is close enough... break and return results

            ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']                                  # get external grid reactive power
            print(branchkey, 'Found Solution ---', step, delta, ex_pgen, ex_qgen)

        except:                                                                                     # if no solution with var limits enforced
            pp.runpp(net, enforce_q_lims=False)                                                     # run straight power flow (ignoring var limits)
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                    # get external grid power
        if abs(ex_pgen) < 1.0:                                                                      # check if external grid is near zero...
            break                                                                                   # delta is close enough... break and return results
        delta += ex_pgen / pfactor_total                                                            # increment delta
        step += 1                                                                                   # increment iteration
        # net.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                           # restore original swshunt qmins
        # net.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                           # restore original swshunt qmaxs

    if solved:
        print('BRANCH {0:9s} ...................................................'.format(branchkey),
              '\u0394 =', round(-1e-3 * best_delta, 5), '(' + str(best_step) + ')', round(best_ex_pgen, 3), round(best_ex_qgen + 0.0, 6))
        return conlabel, bus_results, gen_results, best_delta
    else:
        pp.runopp(net, enforce_q_lims=True)                                                         # run straight power flow
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                        # get external grid real power
        ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']                                      # get external grid reactive power
        print('BRANCH {0:9s} ...................................................'.format(branchkey),
              '\u0394 =', round(-1e-3 * delta, 5), '(' + str(step) + ')', round(ex_pgen, 3), round(ex_qgen + 0.0, 6), 'STILL NOT SOLVED WITH Q-LIMITS')
        return conlabel, net.res_bus, net.res_gen, 0.0


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
    # print('GENERATOR RESULTS')
    # print(_net.res_gen)
    # print()
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
    neta_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/neta.p'
    netc_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/netc.p'
    data_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/netdata.pkl'
    margin_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/margins.pkl'

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
        Outage_dict, Gen_dict, xfmrdict, Pfactor_dict, ext_grid_idx, gids, genbuses, swshidxs, SwShidx_dict, Line_dict, Xfmr_dict, Genidxdict, Swinggen_idxs = pickle.load(PFile)
        PFile.close()
    except FileNotFoundError:
        raise Exception('COULD NOT FIND THIS FILE -->', data_fname)

    # =============================================================================================
    # -- RUN GENERATOR OUTAGES --------------------------------------------------------------------
    # =============================================================================================
    if Outage_dict['gen']:
        print('-------------------- RUNNING GENERATOR OUTAGES ---------------------')
    gopf_starttime = time.time()
    genkeys = Outage_dict['gen'].keys()

    for Genkey in genkeys:                                                                          # LOOP THROUGH GENERATOR OUTAGES
        if Genkey not in Gen_dict:                                                                   # CHECK IF GENERATOR EXISTS
            print('GENERATOR NOT FOUND ................................................', Genkey)   # PRINT MESSAGE
            continue
        Conlabel, bus_results, gen_results, Delta = run_gen_outage_get_delta(Genkey, net_c, net_a, Outage_dict, Pfactor_dict, Gen_dict, SwShidx_dict)
        # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE -----------------------------------
        Conlabel = "'" + Conlabel + "'"                                                             # FORMAT CONLABEL FOR OUTPUT
        write_bus_results(outfname2, bus_results, SwShidx_dict, gen_results, ext_grid_idx, Conlabel)
        write_gen_results(outfname2, gen_results, gids, genbuses, Delta, swshidxs)
    if Outage_dict['gen']:
        print('GENERATOR OUTAGES FINISHED .........................................', round(time.time() - gopf_starttime, 1))

    # =============================================================================================
    # -- RUN LINE AND TRANSFORMER OUTAGES ---------------------------------------------------------
    # =============================================================================================
    if Outage_dict['branch']:
        print('---------------------- RUNNING BRANCH OUTAGES ----------------------')
    bopf_starttime = time.time()

    # -- CALCULATE PARTICIPATING GENERATORS UP MARGIN ---------------------------------------------
    # p_upmargin_total = 0.0                                                                           # initialize gens total reserve
    # p_upmargin_dict = {}                                                                             # initialize gen reserve dict
    # p_downmargin_total = 0.0                                                                           # initialize gens total reserve
    # p_downmargin_dict = {}                                                                             # initialize gen reserve dict
    # for gen_pkey in Pfactor_dict:                                                                  # loop through participating generators
    #     gidx = Gen_dict[gen_pkey]                                                                  # get this participating generator index
    #     if not net_a.gen.loc[gidx, 'in_service']:                                                   # check if participating generator is online...
    #         continue                                                                                # if not... get next participating generator
    #     pgen = net_a.gen.loc[gidx, 'p_kw']                                                          # this generators ratec basecase pgen
    #     pmin = net_a.gen.loc[gidx, 'min_p_kw']                                                      # this generators ratec basecase pmin
    #     pmax =  net_a.gen.loc[gidx, 'max_p_kw']
    #     upmargin = pmin - pgen                                                                        # this generators up reserve margin
    #     downmargin = pmax - pgen
    #     p_upmargin_dict.update({gidx: upmargin})                                                      # update dict... this generators up reserve margin
    #     p_upmargin_total += upmargin                                                                   # increment total up reserve margin
    #     p_downmargin_dict.update({gidx: downmargin})                                                      # update dict... this generators up reserve margin
    #     p_downmargin_total += downmargin                                                                   # increment total up reserve margin
    # Margin_data = [p_upmargin_dict, p_upmargin_total, p_downmargin_dict, p_downmargin_total]

    Pfactor_total = 0.0                                                                             # INITIALIZE FLOAT
    for Gen_pkey in Pfactor_dict:
        gidx = Gen_dict[Gen_pkey]
        if not net_a.gen.loc[gidx, 'in_service']:
            continue
        Pfactor_total += Pfactor_dict[Gen_pkey]                                                     # INCREMENT PFACTOR TOTAL

    branchkeys = Outage_dict['branch'].keys()
    for Branchkey in branchkeys:                                                                    # LOOP THROUGH BRANCH OUTAGES
        if Branchkey not in Line_dict and Branchkey not in Xfmr_dict:                               # CHECK IF BRANCH EXISTS...
            print('LINE OR TRANSFORMER NOT FOUND ......................................', Branchkey)
            continue
        Conlabel, bus_results, gen_results, Delta = run_branch_outage_get_delta(Branchkey, net_c, net_a, Line_dict, Xfmr_dict, Outage_dict, Pfactor_dict, Gen_dict, SwShidx_dict,
                                                                                Pfactor_total, Swinggen_idxs)
        if bus_results is None:
            Conlabel, bus_results, gen_results, Delta = run_robust_branch_outage_get_delta(Branchkey, net_c, net_a, Line_dict, Xfmr_dict, Outage_dict, Pfactor_dict, Gen_dict,
                                                                                           SwShidx_dict, Pfactor_total, Genidxdict, Swinggen_idxs)

        # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE -----------------------------------
        Conlabel = "'" + Conlabel + "'"                                                             # FORMAT CONLABEL FOR OUTPUT
        write_bus_results(outfname2, bus_results, SwShidx_dict, gen_results, ext_grid_idx, Conlabel)
        write_gen_results(outfname2, gen_results, gids, genbuses, Delta, swshidxs)
    if Outage_dict['branch']:
        print('LINE AND TRANSFORMER OUTAGES FINISHED ..............................', round(time.time() - bopf_starttime, 3))

    print('=========================== DONE ===================================')
    print()
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))
