import os
import sys
import csv
import math
import time
import copy
import numpy
import pandapower as pp
from pandas import options as pdoptions
import data as data_read
from itertools import chain
import multiprocessing

cwd = os.path.dirname(__file__)

# -----------------------------------------------------------------------------
# -- USING COMMAND LINE -------------------------------------------------------
# -----------------------------------------------------------------------------
if sys.argv[1:]:
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    MaxRunningTime = float(sys.argv[5])
    ScoringMethod = int(sys.argv[6])
    NetworkModel = sys.argv[7]
    outfname = 'solution1.txt'

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
    # inl_fname = cwd + r'/' + network + r'/' + scenario + r'/case.inl'        # TODO for testing trial results (different directory)
    # rop_fname = cwd + r'/' + network + r'/' + scenario + r'/case.rop'        # TODO for testing trial results (different directory)

    MaxRunningTime = 600.0
    ScoringMethod = 1
    NetworkModel = network + '  ' + scenario

    outfname = cwd + '//solution1.txt'
    try:
        os.remove(outfname)
    except FileNotFoundError:
        pass


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def read_data(raw_name, rop_name, inl_name, con_name):
    """read psse raw data"""
    p = data_read.Data()
    # p = Data()
    print('READING RAW FILE ...................................................', os.path.split(raw_name)[1])
    if raw_name is not None:
        p.raw.read(os.path.normpath(raw_name))
    print('READING ROP FILE ...................................................', os.path.split(rop_name)[1])
    if rop_name is not None:
        p.rop.read(os.path.normpath(rop_name))
    print('READING INL FILE ...................................................', os.path.split(inl_name)[1])
    if inl_name is not None:
        p.inl.read(os.path.normpath(inl_name))
    print('READING CON FILE....................................................', os.path.split(con_name)[1])
    if con_name is not None:
        p.con.read(os.path.normpath(con_name))
    return p


def write_csvdata(fname, lol, label):
    """write csv data to file"""
    with open(fname, 'a', newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_base_bus_results(fname, b_results, sw_dict, g_results, exgridbus):
    """write basecase bus data to file"""
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
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_base_gen_results(fname, g_results, genids, gbuses, swsh_idxs):
    """write basecase generator data to file"""
    g_results.drop(swsh_idxs, inplace=True)
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
    write_csvdata(fname, glist, [['--generator section']])
    return


def get_swingbus_data(buses):
    """get swing bus data"""
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus = 0                                                                                       # initialze int
    swkv = 0.0                                                                                      # initialize float
    swangle = 0.0                                                                                   # initialize float
    # swvlow = 0.0                                                                                    # initialize float
    # swvhigh = 0.0                                                                                   # initialize float
    for xbus in buses.values():                                                                     # loop through buses object
        if xbus.ide == 3:                                                                           # if bustype is swingbus...
            swbus = xbus.i                                                                          # get busnumber
            swkv = xbus.baskv                                                                       # get bus kv
            swangle = 0.0                                                                           # set swinbus angle = 0.0
            # swvhigh = xbus.evhi                                                                     # get swingbus emergency high voltage
            # swvlow = xbus.evlo                                                                      # get swingbus emergency low voltage
            break                                                                                   # exit... found swing bus
    return [swbus, swkv, swangle]


def get_swgens_data(swbus, generators):
    """get swing generator(s) data"""
    # generators = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    swgens_data = []
    del_gens = []
    for gen in generators.values():
        if gen.i == swbus:
            gkey = str(gen.i) + '-' + gen.id
            swgens_data.append([gkey, str(gen.id), float(gen.pg), float(gen.qg), float(gen.qt), float(gen.qb), float(gen.vs), float(gen.pt), float(gen.pb), int(gen.stat)])
            del_gens.append((gen.i, gen.id))
    for gen in del_gens:
        del generators[gen]
    return swgens_data


def copy_opf_to_network(copyfrom_net, copyto_net, gendict, genbusdict, swbus, swshdict, swshbusdict, extgrididx):
    """copy opf results to this network"""
    copyto_net.gen['p_mw'] = copyfrom_net.res_gen['p_mw']                                           # set this network generators power to opf results
    for g_key in gendict:                                                                           # loop across generator keys
        g_idx = gendict[g_key]                                                                      # get generator index
        gen_bus = genbusdict[g_idx]                                                                 # get generator bus
        if gen_bus == swbus:                                                                            # check if swing bus...
            copyto_net.ext_grid.loc[extgrididx, 'vm_pu'] = copyfrom_net.res_bus.loc[gen_bus, 'vm_pu']   # set extgrid vreg
        copyto_net.gen.loc[g_idx, 'vm_pu'] = copyfrom_net.res_bus.loc[gen_bus, 'vm_pu']                 # set this network gens vreg to opf results
    for sh_key in swshdict:                                                                         # loop across swshunt keys
        sh_idx = swshdict[sh_key]                                                                   # get swshunt index
        sh_bus = swshbusdict[sh_idx]                                                                # get swshunt bus
        copyto_net.gen.loc[sh_idx, 'vm_pu'] = copyfrom_net.res_bus.loc[sh_bus, 'vm_pu']             # set this network swshunt vreg to opf results
    return copyto_net


def run_outage_ac(xnet, okey, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, iteration):
    """run powerflow on outage and check for overloads"""
    screen_loading_pct = min(100.0, 80.0 + (5.0 * iteration))
    nosolve_keys = []                                                                               # initialize list
    constrained_branches = {}                                                                       # initialize dict
    net = copy.deepcopy(xnet)                                                                       # get fresh copy of network
    if okey in onlinegens:                                                                          # check if outage is a generator...
        gidx = gendict[okey]                                                                        # get generator index
        pgen = net.res_gen.loc[gidx, 'p_mw']                                                        # get the outaged generator's pgen
        for loadkey in loaddict:                                                                    # loop through the loads
            loadidx = loaddict[loadkey]                                                             # get the load index
            loadmw = net.load.loc[loadidx, 'p_mw']                                                  # get the load power
            net.load.loc[loadidx, 'p_mw'] -= (pgen * loadmw / totalloadp)                           # offset this load with it's portion of the outaged generator
        net.gen.in_service[gidx] = False                                                            # switch off outaged generator
    elif okey in linedict:                                                                          # check if the outage is a line...
        lidx = linedict[okey]                                                                       # get line index
        net.line.in_service[lidx] = False                                                           # switch out outaged line
    elif okey in xfmrdict:                                                                          # check if the outage is a xfmr...
        xidx = xfmrdict[okey]                                                                       # get xfmr index
        net.trafo.in_service[xidx] = False                                                          # switch out outaged xfmr
    try:                                                                                            # try straight powerflow solution
        pp.runpp(net, enforce_q_lims=True)                                                          # run powerflow
    except:                                                                                         # if no solution...
        nosolve_keys.append(okey)                                                                   # add outage key to nosolve keys
        print('no solution running ac outage', okey)
        return constrained_branches, nosolve_keys                                                   # return empty dicts and nosolve keys

    for lkey in linedict:                                                                           # loop across line keys
        lidx = linedict[lkey]                                                                       # get line index
        loading_pct = net.res_line.loc[lidx, 'loading_percent']                                     # get this line loading
        if loading_pct > screen_loading_pct:                                                                     # if loading greater than 100%...
            from_bus = xnet.line.loc[lidx, 'from_bus']                                              # get line frombus
            nom_kv = xnet.bus.loc[from_bus, 'vn_kv']                                                # get line nominal kv
            max_i_ka = xnet.line.loc[lidx, 'max_i_ka']                                              # get line maximum current
            mva_rating = nom_kv * max_i_ka * math.sqrt(3)                                           # calculate line mva rating
            mva = loading_pct * mva_rating / 100.0                                                  # calculate mva flow
            mva_overloading = mva - mva_rating                                                 # calculate mva overloading
            if lkey not in constrained_branches:                                                    # if line is in constrained branches...
                constrained_branches.update({lkey: [[mva_overloading, okey]]})                      # add line and data to constrained branches
            else:                                                                                   # if line already in constrained branches...
                constrained_branches[lkey].append([mva_overloading, okey])                          # add mva overloading and outagekey to line key
    for xkey in xfmrdict:                                                                           # loop across xfmr keys
        xidx = xfmrdict[xkey]                                                                       # get xfmr index
        loading_pct = net.res_trafo.loc[xidx, 'loading_percent']                                    # get this xfmr loading
        if loading_pct > screen_loading_pct:                                                                     # if loading greater than 100%...
            mva_rating = xnet.trafo.loc[xidx, 'sn_mva']                                             # get mva rating
            mva = loading_pct * mva_rating / 100.0                                                  # get mva flow
            mva_overloading = mva - mva_rating                                                 # calculate mva overloading
            if xkey not in constrained_branches:                                                    # if xfmr is in constrained branches...
                constrained_branches.update({xkey: [[mva_overloading, okey]]})                      # add xfmr and data to constrained branches
            else:                                                                                   # if xfmr already in constrained branches...
                constrained_branches[xkey].append([[mva_overloading, okey]])                        # add mva overloading and outagekey to xfmr key
    return constrained_branches, nosolve_keys


def arghelper1(args):
    """ multiprocessing argument helper """
    return run_outage_ac(*args)


def parallel_run_outage_ac(arglist):
    """" prepare group data for parallel screening """
    numcpus = int(os.environ['NUMBER_OF_PROCESSORS'])
    pool = multiprocessing.Pool(processes=numcpus)
    results = pool.map(arghelper1, arglist)
    pool.close()
    pool.join()
    return results


def get_dominant_outages_ac(xnet, outagekeys, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, iteration):
    """get dominant outages resulting in branch loading, calls run_outages for possible multiprocessing"""
    Parallel_Processing = True
    nosolve_keys = []                                                                               # initialize list
    constrained_branch_dict = {}                                                                    # declare dict
    for lkey in linedict:                                                                           # loop through lines
        constrained_branch_dict.update({lkey: []})                                                  # initialize constrained branch dict
    for xkey in xfmrdict:                                                                           # loop through xfmrs
        constrained_branch_dict.update({xkey: []})                                                  # initialize constrained branch dict

    if not Parallel_Processing:                                                                                                   # if serial processing...
        for okey in outagekeys:                                                                                                  # loop across outages
            limited_branches, nskeys = run_outage_ac(xnet, okey, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, iteration)  # run outage in function
            for limited_branch in limited_branches:
                constrained_branch_dict[limited_branch] += limited_branches[limited_branch]
            nosolve_keys += nskeys                                                                                                # update nosolve keys

    if Parallel_Processing:                                                                                                       # if parallel processing...
        arglist = [[xnet, x, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, iteration] for x in outagekeys]                  # get argument list for each process
        results = parallel_run_outage_ac(arglist)                                                                                 # get parallel results
        results = list(zip(*results))                                                                                             # transpose results
        limited_branches_i, nskeys_i = results                                                                                    # break out results
        for limited_branches in limited_branches_i:                                                                                 # loop across each result limited branches
            for limited_branch in limited_branches:
                constrained_branch_dict[limited_branch] += limited_branches[limited_branch]

    # -- REMOVE EMPTY DICT LISTS AND SORT REMAINING DICT LISTS ------------------------------------
    tempdict = {}                                                                                   # initialize tempdict
    for bkey in constrained_branch_dict:                                                            # loop across constrained branch dict
        if constrained_branch_dict[bkey]:                                                           # if dict list value is not empty...
            constrained_branch_dict[bkey].sort(reverse=True)                                        # sort list and add to tempdict
            tempdict.update({bkey: constrained_branch_dict[bkey]})                                  # add key:[list] to tempdict
    constrained_branch_dict = tempdict                                                              # reassign dict

    worst_constraints = []                                                                          # declare list of worst contraints
    for bkey in constrained_branch_dict:                                                            # loop across constrained branches
        worst_constraints.append(constrained_branch_dict[bkey][0])                                  # add worst constraint for this branch
    worst_constraints.sort(reverse=True)                                                            # sort largest to smallest contraint

    # -- SEPARATE INTO DOMINANT OUTAGES,OVERLOADS -------------------------------------------------
    dominantoutages = []                                                                            # initialize list
    dominantmvaoverloads = []                                                                       # initialize list
    numoverloads = 0
    for data in worst_constraints:                                                                  # loop across worst contraints
        mva_overload = data[0]                                                                      # get mva overloading
        outage = data[1]                                                                            # get outage causing the overloading
        if outage not in dominantoutages:                                                           # if this outage not in dominant outages
            dominantoutages.append(outage)                                                          # add outage to dominant outages
            dominantmvaoverloads.append(round(mva_overload, 1))                                     # add mva overloading to dominant overloads
            if mva_overload > 0.0:
                numoverloads += 1
    totaloverloading = sum([x for x in dominantmvaoverloads if x > 0.0])

    print()
    print('{0:<2d} DOMINANT OUTAGES'.format(iteration), dominantoutages, 'NOSOLVES =', nosolve_keys)
    print('{0:<2d} LOADING OVER MAX'.format(iteration), dominantmvaoverloads, '[{0:d} OVERLOADS = {1:.1f} MVA]'.format(numoverloads, totaloverloading))
    return dominantoutages, numoverloads


def get_df_dict(xnet, goutagekeys, boutagekeys, gendict, linedict, xfmrdict, loaddict, totalloadp):
    """calculate generator and branch outage distribution factors"""
    df_dict = {}                                                                                    # declare distribution factor dict
    pp.runpp(xnet, enforce_q_lims=True)                                                             # run powerflow on basecase
    for okey in goutagekeys:                                                                        # loop through generator outage keys
        df_dict.update({okey: {}})                                                                  # initialize distribution factor dict of dicts
    for okey in boutagekeys:                                                                        # loop through branch outage keys
        df_dict.update({okey: {}})                                                                  # initialize distribution factor dict of dicts

    # -- LOOP THROUGH GENERATOR OUTAGES -----------------------------------------------------------
    for okey in goutagekeys:                                                                        # loop through generator outage keys
        net = copy.deepcopy(xnet)                                                                   # get copy of base network
        gidx = gendict[okey]                                                                        # get generator index to outage
        pgen = xnet.res_gen.loc[gidx, 'p_mw']                                                       # get pre-outage generator mw
        for loadkey in loaddict:                                                                    # loop through the loads
            loadidx = loaddict[loadkey]                                                             # get the load index
            loadmw = net.load.loc[loadidx, 'p_mw']                                                  # get the load power
            net.load.loc[loadidx, 'p_mw'] -= (pgen * loadmw / totalloadp)                           # offset this load with it's portion of the outaged generator
        net.gen.in_service[gidx] = False                                                            # switch off outaged generator
        try:                                                                                        # try to run powerflow on outage
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow on outage
        except:                                                                                     # try to run powerflow again
            try:
                pp.runpp(net, enforce_q_lims=False)                                                     # run powerflow on outage (ignore Q limits)
                print('   Q LIMITS IGNORED FOR OUTAGE .....................................', okey)
            except:
                print('   DID NOT CONVERGE ... GET NEXT OUTAGE ............................', okey)
                for lkey in linedict:
                    df_dict[okey].update({lkey: 0.0})
                for xkey in xfmrdict:
                    df_dict[okey].update({xkey: 0.0})
                continue

        # -- FOR GEN OUTAGE... LOOP ACROSS MONITORED BRANCHES AND GET DISTRUBUTION FACTORS --------
        for lkey in linedict:                                                                       # loop through network line keys
            lidx = linedict[lkey]                                                                   # get line index
            pre_mw = xnet.res_line.loc[lidx, 'p_from_mw']                                           # get pre-outage line mw (from bus)
            post_mw = net.res_line.loc[lidx, 'p_from_mw']                                           # get post-outage line mw (from bus)
            df = (post_mw - pre_mw) / pgen                                                          # calculate distribution factor for this outage, this branch
            if df < -1.0:                                                                           # limit distribution factors to +- 1.0
                df = -1.0                                                                           #
            elif df > 1.0:                                                                          #
                df = 1.0                                                                            #
            df_dict[okey].update({lkey: df})                                                        # update distribution factor dict of dicts
        for xkey in xfmrdict:                                                                       # loop through network xfmr keys
            xidx = xfmrdict[xkey]                                                                   # get xfmr index
            pre_mw = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                            # get pre-outage xfmr mw (high bus)
            post_mw = net.res_trafo.loc[xidx, 'p_hv_mw']                                            # get post-outage xfmr mw (high bus)
            df = (post_mw - pre_mw) / pgen                                                          # calculate distribution factor for this outage, this branch
            if df < -1.0:                                                                           # limit distribution factors to +- 1.0
                df = -1.0                                                                           #
            elif df > 1.0:                                                                          #
                df = 1.0                                                                            #
            df_dict[okey].update({xkey: df})                                                        # update distribution factor dict of dicts

    # -- LOOP THROUGH BRANCH OUTAGES --------------------------------------------------------------
    for okey in boutagekeys:                                                                        # loop through branch outage keys
        net = copy.deepcopy(xnet)                                                                   # get copy of base network
        if okey in linedict:                                                                        # check if line outage...
            lidx = linedict[okey]                                                                   # get line index
            branch_mw = xnet.res_line.loc[lidx, 'p_from_mw']                                        # get pre-outage line mw (from bus)
            if branch_mw == 0.0:                                                                    # if line flow is zero...
                continue                                                                            # get the next branch
            net.line.in_service[lidx] = False                                                       # switch line out of service
        elif okey in xfmrdict:                                                                      # check if xfmr outage...
            xidx = xfmrdict[okey]                                                                   # get xfmr index
            branch_mw = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                         # get pre-outage xfmr mw (high bus)
            if branch_mw == 0.0:                                                                    # if xfmr flow is zero...
                continue                                                                            # get the next branch
            net.trafo.in_service[xidx] = False                                                      # switch xfmr out of service
        try:                                                                                        # try to run powerflow on outage
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow on outage
        except:                                                                                     # try to run powerflow again
            try:
                pp.runpp(net, enforce_q_lims=False)                                                 # run powerflow on outage (ignore Q limits)
                print('   Q LIMITS IGNORED FOR OUTAGE .....................................', okey)
            except:
                print('   DID NOT CONVERGE ... GET NEXT OUTAGE ............................', okey)
                for lkey in linedict:
                    df_dict[okey].update({lkey: 0.0})
                for xkey in xfmrdict:
                    df_dict[okey].update({xkey: 0.0})
                continue

        # -- FOR BRANCH OUTAGE... LOOP ACROSS MONITORED BRANCHES AND GET DISTRUBUTION FACTORS -----
        for lkey in linedict:                                                                       # loop through network line keys
            if lkey == okey:                                                                        # check if monitored line is the outaged line...
                continue                                                                            # if so, get next monitored line
            lidx = linedict[lkey]                                                                   # get line index
            pre_mw = xnet.res_line.loc[lidx, 'p_from_mw']                                           # get pre-outage line mw (from bus)
            post_mw = net.res_line.loc[lidx, 'p_from_mw']                                           # get post-outage line mw (from bus)
            df = (post_mw - pre_mw) / branch_mw                                                     # calculate distribution factor for this outage, this branch
            if df < -1.0:                                                                           # limit distribution factors to +- 1.0
                df = -1.0                                                                           #
            elif df > 1.0:                                                                          #
                df = 1.0                                                                            #
            df_dict[okey].update({lkey: df})                                                        # update distribution factor dict of dicts
        for xkey in xfmrdict:                                                                       # loop through network line keys
            if xkey == okey:                                                                        # check if monitored line is the outaged line...
                continue                                                                            # if so, get next monitored line
            xidx = xfmrdict[xkey]                                                                   # get line index
            pre_mw = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                            # get pre-outage xfmr mw (high bus)
            post_mw = net.res_trafo.loc[xidx, 'p_hv_mw']                                            # get post-outage xfmr mw (high bus)
            df = (post_mw - pre_mw) / branch_mw                                                     # calculate distribution factor for this outage, this branch
            if df < -1.0:                                                                           # limit distribution factors to +- 1.0
                df = -1.0                                                                           #
            elif df > 1.0:                                                                          #
                df = 1.0                                                                            #
            df_dict[okey].update({xkey: df})                                                        # update distribution factor dict of dicts
    return df_dict


def get_dominant_outages_df(xnet, outagekeys, onlinegens, gendict, linedict, xfmrdict, dfdict, iteration):
    """use distribution factor to get dominant outages resulting in branch loading"""
    screen_loading_pct = min(100.0, 80.0 + (5.0 * iteration))
    constrained_branch_dict = {}                                                                    # declare dict
    for lkey in linedict:                                                                           # loop through lines
        constrained_branch_dict.update({lkey: []})                                                  # initialize constrained branch dict
    for xkey in xfmrdict:                                                                           # loop through xfmrs
        constrained_branch_dict.update({xkey: []})                                                  # initialize constrained branch dict

    for okey in outagekeys:
        # -- CHECK IF OUTAGE IS A GENERATOR -----------------------------------------------------------
        if okey in onlinegens:                                                                          # check if outage is a generator...
            gidx = gendict[okey]                                                                        # get generator index
            pgen = xnet.res_gen.loc[gidx, 'p_mw']                                                       # get pre-outage generator mw
            # -- LOOP ACROSS MONITORED LINES --------------------------------------
            for lkey in linedict:                                                                       # loop through monitored line keys
                df = dfdict[okey][lkey]                                                                 # get distribution factor for this outage, this line
                lidx = linedict[lkey]                                                                   # get line index
                from_bus = xnet.line.loc[lidx, 'from_bus']                                              # get line frombus
                nom_kv = xnet.bus.loc[from_bus, 'vn_kv']                                                # get line nominal kv
                max_i_ka = xnet.line.loc[lidx, 'max_i_ka']                                              # get line maximum current
                mva_rating = nom_kv * max_i_ka * math.sqrt(3)                                           # calculate line mva rating
                pre_mva = xnet.res_line.loc[lidx, 'p_from_mw']                                          # get pre-outage branch mw
                mva = abs(pre_mva + (df * pgen))                                                        # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[lkey].append([mva_overloading, okey])                      # add mva overloading and outagekey to line key

            # -- LOOP ACROSS MONITORED XFMRS --------------------------------------
            for xkey in xfmrdict:                                                                       # loop through monitored xfmr keys
                df = dfdict[okey][xkey]                                                                 # get distribution factor for this outage, this xfmr
                xidx = xfmrdict[xkey]                                                                   # get xfmr index
                mva_rating = xnet.trafo.loc[xidx, 'sn_mva']                                             # get xfmr rating
                pre_mva = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                           # get pre-outage xfmr mw (high bus)
                mva = abs(pre_mva + (df * pgen))                                                        # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[xkey].append([[mva_overloading, okey]])                    # add mva overloading and outagekey to xfmr key

        # -- CHECK IF OUTAGE IS A LINE ----------------------------------------------------------------
        elif okey in linedict:                                                                          # check if outage is a line...
            lidx = linedict[okey]                                                                       # get outaged line index
            branch_mw = xnet.res_line.loc[lidx, 'p_from_mw']                                            # get pre-outage line mw (from bus)
            if branch_mw == 0.0:                                                                        # if pre-outage line flow is zero...
                continue
            # -- LOOP ACROSS MONITORED LINES --------------------------------------
            for lkey in linedict:                                                                       # loop through monitored line keys
                if lkey == okey:                                                                        # check if monitored line is the outaged line...
                   continue
                df = dfdict[okey][lkey]                                                                 # get distribution factor for this outage, this line
                lidx = linedict[lkey]                                                                   # get line index
                from_bus = xnet.line.loc[lidx, 'from_bus']                                              # get line frombus
                nom_kv = xnet.bus.loc[from_bus, 'vn_kv']                                                # get line nominal kv
                max_i_ka = xnet.line.loc[lidx, 'max_i_ka']                                              # get line maximum current
                mva_rating = nom_kv * max_i_ka * math.sqrt(3)                                           # calculate line mva rating
                pre_mva = xnet.res_line.loc[lidx, 'p_from_mw']                                          # get pre-outage branch mw
                mva = abs(pre_mva + (df * branch_mw))                                                   # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[lkey].append([mva_overloading, okey])                      # add mva overloading and outagekey to line key

            # -- LOOP ACROSS MONITORED XFMRS --------------------------------------
            for xkey in xfmrdict:                                                                       # loop through monitored xfmr keys
                df = dfdict[okey][xkey]                                                                 # get distribution factor for this outage, this xfmr
                xidx = xfmrdict[xkey]                                                                   # get xfmr index
                mva_rating = xnet.trafo.loc[xidx, 'sn_mva']                                             # get xfmr rating
                pre_mva = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                           # get pre-outage xfmr mw (high bus)
                mva = abs(pre_mva + (df * branch_mw))                                                   # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[xkey].append([[mva_overloading, okey]])                    # add mva overloading and outagekey to xfmr key

        # -- CHECK IF OUTAGE IS A TRANSFORMER ---------------------------------------------------------
        elif okey in xfmrdict:                                                                          # check if outage is a xfmr...
            xidx = xfmrdict[okey]                                                                   # get outaged line index
            branch_mw = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                         # get pre-outage xfmr mw (high bus)
            if branch_mw == 0.0:                                                                        # if pre-outage xfmr flow is zero...
                continue
            # -- LOOP ACROSS MONITORED LINES --------------------------------------
            for lkey in linedict:                                                                       # loop through monitored line keys
                df = dfdict[okey][lkey]                                                                 # get distribution factor for this outage, this line
                lidx = linedict[lkey]                                                                   # get line index
                from_bus = xnet.line.loc[lidx, 'from_bus']                                              # get line frombus
                nom_kv = xnet.bus.loc[from_bus, 'vn_kv']                                                # get line nominal kv
                max_i_ka = xnet.line.loc[lidx, 'max_i_ka']                                              # get line maximum current
                mva_rating = nom_kv * max_i_ka * math.sqrt(3)                                           # calculate line mva rating
                pre_mva = xnet.res_line.loc[lidx, 'p_from_mw']                                          # get pre-outage branch mw
                mva = abs(pre_mva + (df * branch_mw))                                                   # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[lkey].append([mva_overloading, okey])                      # add mva overloading and outagekey to line key

            # -- LOOP ACROSS MONITORED XFMRS --------------------------------------
            for xkey in xfmrdict:                                                                       # loop through monitored xfmr keys
                if xkey == okey:                                                                        # check if monitored xfmr is the outaged xfmr...
                    continue
                df = dfdict[okey][xkey]                                                                 # get distribution factor for this outage, this xfmr
                xidx = xfmrdict[xkey]                                                               # get xfmr index
                mva_rating = xnet.trafo.loc[xidx, 'sn_mva']                                         # get xfmr rating
                pre_mva = xnet.res_trafo.loc[xidx, 'p_hv_mw']                                       # get pre-outage xfmr mw (high bus)
                mva = abs(pre_mva + (df * branch_mw))                                                   # calculate post-outage branch flow
                loading_pct = 100.0 * mva / mva_rating                                                  # calculate loading percent
                if loading_pct > screen_loading_pct:                                                    # if loading greater than 100%...
                    mva_overloading = mva - mva_rating                                                  # calculate mva overloading
                    constrained_branch_dict[xkey].append([[mva_overloading, okey]])                    # add mva overloading and outagekey to xfmr key

    # -- REMOVE EMPTY DICT LISTS AND SORT REMAINING DICT LISTS ------------------------------------
    tempdict = {}                                                                                   # initialize tempdict
    for bkey in constrained_branch_dict:                                                            # loop across constrained branch dict
        if constrained_branch_dict[bkey]:                                                           # if dict list value is not empty...
            constrained_branch_dict[bkey].sort(reverse=True)                                        # sort list and add to tempdict
            tempdict.update({bkey: constrained_branch_dict[bkey]})                                  # add key:[list] to tempdict
    constrained_branch_dict = tempdict                                                              # reassign dict

    worst_constraints = []                                                                          # declare list of worst contraints
    for bkey in constrained_branch_dict:                                                            # loop across constrained branches
        worst_constraints.append(constrained_branch_dict[bkey][0])                                  # add worst constraint for this branch
    worst_constraints.sort(reverse=True)                                                            # sort largest to smallest contraint

    # -- SEPARATE INTO DOMINANT OUTAGES,OVERLOADS -------------------------------------------------
    dominantoutages = []                                                                            # initialize list
    dominantmvaoverloads = []                                                                       # initialize list
    numoverloads = 0
    for data in worst_constraints:                                                                  # loop across worst contraints
        mva_overload = data[0]                                                                      # get mva overloading
        outage = data[1]                                                                            # get outage causing the overloading
        if outage not in dominantoutages:                                                           # if this outage not in dominant outages
            dominantoutages.append(outage)                                                          # add outage to dominant outages
            dominantmvaoverloads.append(round(mva_overload, 1))                                     # add mva overloading to dominant overloads
            if mva_overload > 0.0:
                numoverloads += 1
    totaloverloading = sum([x for x in dominantmvaoverloads if x > 0.0])

    print()
    print('{0:<2d} DOMINANT OUTAGES'.format(iteration), dominantoutages)
    print('{0:<2d} LOADING OVER MAX'.format(iteration), dominantmvaoverloads, '[{0:d} OVERLOADS = {1:.1f} MVA]'.format(numoverloads, totaloverloading))
    return dominantoutages, numoverloads


def arghelper2(args):
    """ multiprocessing argument helper """
    return run_outage_opf(*args)


def parallel_run_outage_opf(arglist):
    """" prepare outage data for parallel processing """
    numcpus = int(os.environ['NUMBER_OF_PROCESSORS'])
    pool = multiprocessing.Pool(processes=numcpus)
    results = pool.map(arghelper2, arglist)
    pool.close()
    pool.join()
    return results


def run_outage_opf(xnet, okey, participatinggens, gendict, linedict, xfmrdict, genbusdict, swbus, swshdict, swshbusdict, extgrididx):
    """run opf on outage and return generator pgens"""
    opfpgendict = {okey: {}}
    net = copy.deepcopy(xnet)                                                                       # get fresh copy of this master network
    if okey in gendict:                                                                             # check if a generator...
        gidx = gendict[okey]                                                                        # get generator index
        net.gen.in_service[gidx] = False                                                            # switch off outaged generator
    elif okey in linedict:                                                                          # check if a line...
        lidx = linedict[okey]                                                                       # get line index
        net.line.in_service[lidx] = False                                                           # switch out outaged line
    elif okey in xfmrdict:                                                                          # check if a xfmr...
        xidx = xfmrdict[okey]                                                                       # get xfmr index
        net.trafo.in_service[xidx] = False                                                          # switch out outaged xfmr
    try:                                                                                            # try to run powerflow
        pp.runpp(net, enforce_q_lims=True)                                                          # solve this network with powerflow
    except:                                                                                         # if powerflow did not solve
        print('Q LIMITS IGNORED')                                                                   # print statement
        pp.runpp(net, enforce_q_lims=False)                                                         # solve this network with powerflow (ignore q limits)
    try:                                                                                            # try opf powerflow solution
        pp.runopp(net, init='pf')                                                                   # run opf on this network
    except:                                                                                         # if no solution...
        print('OUTAGE DID NOT SOLVE WITH OPF ....', okey, '.... SKIP, GET NEXT OUTAGE')             # print nosolve message
        return opfpgendict                                                                          # get next contingency

    net = copy_opf_to_network(net, net, gendict, genbusdict, swbus, swshdict, swshbusdict, extgrididx)  # copy opf results to this network
    pp.runpp(net, enforce_q_lims=True)                                                                  # solve this network with powerflow
    opfpgendict[okey] = get_generator_pgens(net, participatinggens, gendict)                                   # get generators pgen for this master basecase
    return opfpgendict


def get_generator_pgens(xnet, onlinegens, gendict):
    """get generators pgen for this network"""
    basepgendict = {}                                                                               # initialize dict
    for g_key in onlinegens:                                                                        # loop across generator keys
        g_idx = gendict[g_key]                                                                      # get generator index
        pgen = xnet.res_gen.loc[g_idx, 'p_mw']                                                          # get generator's pgen
        basepgendict.update({g_key: pgen})                                                          # update dict with {gkey:pgen}
    return basepgendict


def get_generation_cost(xnet, participatinggens, gendict, pwlcostdict0):
    """get total generation cost for this network"""
    gcost = 0.0                                                                                     # initialize float
    for g_key in participatinggens:                                                                 # loop across participating generators
        g_idx = gendict[g_key]                                                                      # get generator's index
        pcost_data = pwlcostdict0[g_key]                                                            # get generator's pwl cost data
        g_mw = xnet.res_gen.loc[g_idx, 'p_mw']                                                      # get generator's mw output
        xlist, ylist = zip(*pcost_data)                                                             # transpose pwl cost data
        gcost += numpy.interp(g_mw, xlist, ylist)                                                   # get this gen's cost and add to total
    return gcost


def get_maxloading(xnet, linedict, xfmrdict):
    """get max line or xfmr loading for this network"""
    max_loading = 0.0
    bkey = ''
    for lkey in linedict:
        lidx = linedict[lkey]
        loading = xnet.res_line.loc[lidx, 'loading_percent']
        if loading > max_loading:
            max_loading = loading
            bkey = lkey
    for xkey in xfmrdict:
        xidx = xfmrdict[xkey]
        loading = xnet.res_trafo.loc[xidx, 'loading_percent']
        if loading > max_loading:
            max_loading = loading
            bkey = xkey
    max_loading = round(max_loading, 3)
    return [max_loading, bkey]


def get_minmax_voltage(xnet):
    """get max and min bus voltage for this network"""
    min_voltage = min(xnet.res_bus['vm_pu'].values)                                                 # get max bus voltage
    max_voltage = max(xnet.res_bus['vm_pu'].values)                                                 # get min bus voltage
    return min_voltage, max_voltage


def print_dataframes_results(_net):                                                                 # TODO DEVELOPMENT --------------------------------
    """development... print results of this network"""
    pdoptions.display.max_columns = 100
    pdoptions.display.max_rows = 10000
    pdoptions.display.max_colwidth = 100
    pdoptions.display.width = None
    pdoptions.display.precision = 6
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
    # print()
    # print('GENERATOR DATAFRAME')
    # print(_net.gen)
    # print()
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


# =================================================================================================
# -- MYPYTHON_1 -----------------------------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    master_start_time = time.time()                                                                 # INITIALIZE MAIN PROGRAM START TIME
    print()                                                                                         # PRINT STATEMENT
    print('===================  ' + NetworkModel + '  ===================')
    print('MAX RUNNING TIME =', MaxRunningTime)
    print('SCORING METHOD =', ScoringMethod)

    MaxLoading = 95.0                                                                               # MAXIMUM %BRANCH LOADING FOR N-0 AND N-1
    UseNetC = False                                                                                 # FLAG FOR WHICH NETWORK TO USE FOR SCOPF
    if UseNetC:                                                                                     # IF USING NETC FOR SCOPF...
        MaxMinBusVoltageAdj = 0.015                                                                 # SET NETC MIN, MAX BUS VOLTAGE ADJUSTMENT

    # =============================================================================================
    # -- GET RAW,ROP,INL,CON DATA FROM FILES ------------------------------------------------------
    # =============================================================================================
    print('------------------------- READING RAW DATA -------------------------')
    areas = []
    gdisp_dict = {}
    pdisp_dict = {}
    pwl_dict0 = {}
    pwl_dict = {}
    pwlcost_dict0 = {}
    pwlcost_dict = {}
    outage_dict = {'branch': {}, 'gen': {}}
    pfactor_dict = {}
    pwl_map_dict = {}
    max_pwl_shape = 0
    raw_data = read_data(raw_fname, rop_fname, inl_fname, con_fname)

    # -- GET BASE MVA -----------------------------------------------------------------------------
    mva_base = raw_data.raw.case_identification.sbase

    # -- GET NETWORK AREAS ------------------------------------------------------------------------
    for area in raw_data.raw.areas.values():
        areas.append(area.i)

    # -- GET ACTIVE POWER DISPATCH TABLES FROM GEN DISPATCH DATA ----------------------------------
    for gdisp in raw_data.rop.generator_dispatch_records.values():
        gkey = str(gdisp.bus) + '-' + str(gdisp.genid)
        gdisp_dict.update({gkey: gdisp.dsptbl})

    # -- GET PWL TABLES FROM ACTIVE POWER DISPATCH TABLES -----------------------------------------
    for pdisp in raw_data.rop.active_power_dispatch_records.values():
        pdisp_dict.update({pdisp.tbl: pdisp.ctbl})

    # -- GET PWL DATA FROM PWL COST TABLES --------------------------------------------------------
    for pwldata in raw_data.rop.piecewise_linear_cost_functions.values():
        pwl_dict0.update({pwldata.ltbl: []})
        pwl_dict.update({pwldata.ltbl: []})
        for pair in pwldata.points:
            pwl_dict0[pwldata.ltbl].append([pair.x, pair.y])

    for tbl in pwl_dict0:
        for j in range(len(pwl_dict0[tbl]) - 1):
            x0 = pwl_dict0[tbl][j][0]
            y0 = pwl_dict0[tbl][j][1]
            x1 = pwl_dict0[tbl][j + 1][0]
            y1 = pwl_dict0[tbl][j + 1][1]
            slope = (y1 - y0) / (x1 - x0)
            pwl_dict[tbl].append([x0, x1, slope])

    for gkey in gdisp_dict:
        disptablekey = gdisp_dict[gkey]
        costtablekey = pdisp_dict[disptablekey]
        pcostdata0 = pwl_dict0[costtablekey]
        pcostdata = pwl_dict[costtablekey]
        pwlcost_dict0.update({gkey: pcostdata0})
        pwlcost_dict.update({gkey: pcostdata})

    # -- GET GENERATOR PARTICIPATION FACTORS ------------------------------------------------------
    for pf_record in raw_data.inl.generator_inl_records.values():
        gkey = str(pf_record.i) + '-' + pf_record.id
        pfactor_dict.update({gkey: pf_record.r})

    # -- GET CONTINGENCY DATA ---------------------------------------------------------------------
    for con in raw_data.con.contingencies.values():
        clabel = con.label
        for event in con.branch_out_events:
            ibus = event.i
            jbus = event.j
            ckt = event.ckt
            bkey = str(ibus) + '-' + str(jbus) + '-' + ckt
            outage_dict['branch'].update({bkey: clabel})
        for event in con.generator_out_events:
            gbus = event.i
            gid = event.id
            gkey = str(gbus) + '-' + gid
            outage_dict['gen'].update({gkey: clabel})

    # -- GET SWING BUS FROM RAW BUSDATA -----------------------------------------------------------
    swingbus, swing_kv, swing_angle = get_swingbus_data(raw_data.raw.buses)

    # -- GET SWING GEN DATA FROM GENDATA (REMOVE SWING GEN FROM GENDATA) --------------------------
    swgens_data = get_swgens_data(swingbus, raw_data.raw.generators)

    # =============================================================================================
    # == CREATE NETWORK ===========================================================================
    # =============================================================================================
    print('------------------------ CREATING NETWORKS -------------------------')
    create_starttime = time.time()
    net_a = pp.create_empty_network('net_a', 60.0, mva_base)
    if UseNetC:
        net_c = pp.create_empty_network('net_c', 60.0, mva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.name, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    bus_dict = {}
    busnomkv_dict = {}
    buskv_dict = {}
    busarea_dict = {}
    busidxs = []
    areas = []
    for bus in raw_data.raw.buses.values():
        busnum = bus.i
        busnomkv = bus.baskv
        busarea = bus.area
        buskv = bus.vm
        if busnum == swingbus:
            sw_vmax_a = bus.nvhi
            sw_vmin_a = bus.nvlo
            sw_vmax_c = bus.evhi
            sw_vmin_c = bus.evlo
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_bus(net_a, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.nvhi - 0.005, min_vm_pu=bus.nvlo + 0.005, in_service=True, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        if UseNetC:

            idx = pp.create_bus(net_c, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.evhi - MaxMinBusVoltageAdj, min_vm_pu=bus.evlo + MaxMinBusVoltageAdj, in_service=True, index=busnum)
        if busnum == swingbus:
            swingbus_idx = idx
        bus_dict.update({busnum: [round(bus.nvlo, 5), round(bus.nvhi, 5)]})
        busnomkv_dict.update({busnum: busnomkv})
        buskv_dict.update({busnum: buskv})
        busarea_dict.update({busnum: busarea})
        busidxs.append(idx)
        areas.append(busarea)
    areas = list(set(areas))
    areas.sort()

    # == ADD LOADS TO NETWORK =====================================================================
    print('ADD LOADS ..........................................................')
    total_loadp = 0.0
    load_keyidx = {}
    # loads = (load.i, load.id, load.status, load.pl, load.ql)
    for load in raw_data.raw.loads.values():
        status = bool(load.status)
        if not status:
            continue
        loadbus = load.i
        loadid = load.id
        loadkey = str(loadbus) + '-' + loadid
        loadp = load.pl
        loadq = load.ql
        loadmva = math.sqrt(loadp ** 2 + loadq ** 2)
        if loadp < 0.0:
            idx = pp.create_sgen(net_a, loadbus, p_mw=-loadp, q_mvar=-loadq, sn_mva=loadmva, name=loadkey)
            if UseNetC:
                idx = pp.create_sgen(net_c, loadbus, p_mw=-loadp, q_mvar=-loadq, sn_mva=loadmva, name=loadkey)
        else:
            idx = pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadkey, controllable=False)
            if UseNetC:
                idx = pp.create_load(net_c, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadkey, controllable=False)
            if status:
                load_keyidx.update({loadkey: idx})
                total_loadp += loadp

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    Gids = []
    gen_keyidx = {}
    gen_idxkey = {}
    swinggen_dict = {}
    genidx_dict = {}
    swinggen_keyidx = {}
    genarea_dict = {}
    genidxs = []
    genbus_dict = {}
    participating_gens = []
    area_participating_gens = {}
    for area in areas:
        area_participating_gens.update({area: []})
    plant_dict = {}
    zero_gens = []
    online_gens = []

    # -- ADD SWING GENERATORS ---------------------------------------------------------------------
    #  swgens_data = (key, id, pgen, qgen, qmax, qmin, vreg, pmax, pmin, status)
    for swgen_data in swgens_data:
        swing_kv = busnomkv_dict[swingbus]
        genbus = swingbus
        genkey = swgen_data[0]
        gid = swgen_data[1]
        pgen = swgen_data[2]
        qgen = swgen_data[3]
        qmax = swgen_data[4]
        qmin = swgen_data[5]
        pmax = swgen_data[7]
        pmin = swgen_data[8]
        vreg = swgen_data[6]
        status = swgen_data[9]
        if not status:
            pgen = 0.0
            qgen = 0.0
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        if genkey in pwlcost_dict and status and pmax > 0.0:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            if UseNetC:
                pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                              rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status, index=idx, slack=False)
                pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            participating_gens.append(genkey)
            area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv, type='SWGEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK
            if UseNetC:
                pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                              rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv,  type='SWGEN', controllable=False, in_service=status, index=idx)
        swing_vreg = vreg
        swinggen_dict.update({genkey: idx})
        gen_keyidx.update({genkey: idx})
        gen_idxkey.update({idx: genkey})
        genbuses.append(genbus)
        Gids.append("'" + gid + "'")
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genbus_dict.update({idx: genbus})
        if status and pmax > 0.0:
            online_gens.append(genkey)
        if pgen == 0.0:
            zero_gens.append(genkey)
        if genbus not in genidx_dict:
            genidx_dict.update({genbus: [idx]})
        else:
            genidx_dict[genbus].append(idx)
        if genbus not in plant_dict:
            if pgen > 0.0:
                plant_dict.update({genbus: [[pgen, genkey]]})
        else:
            if pgen > 0.0:
                plant_dict[genbus].append([pgen, genkey])

        # -- ADD REMAINING GENERATOR ------------------------------------------------------------------
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    for gen in raw_data.raw.generators.values():
        genbus = gen.i
        gid = gen.id
        pgen = gen.pg
        qgen = gen.qg
        qmax = gen.qt
        qmin = gen.qb
        pmax = gen.pt
        pmin = gen.pb
        vreg = gen.vs
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        status = bool(gen.stat)
        if not status:
            pgen = 0.0
            qgen = 0.0
        pcostdata = None
        genkey = str(genbus) + '-' + str(gid)
        if genkey in pwlcost_dict and status and pmax > 0.0:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv, type='GEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            if UseNetC:
                pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                              rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv,  type='GEN', controllable=True, in_service=status, index=idx)
                pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            participating_gens.append(genkey)
            area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            if UseNetC:
                pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                              rdss_pu=0.005, xdss_pu=0.25, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status, index=idx)
        Gids.append("'" + gid + "'")
        genbuses.append(genbus)
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        gen_keyidx.update({genkey: idx})
        gen_idxkey.update({idx: genkey})
        genbus_dict.update({idx: genbus})
        if status and pmax > 0.0:
            online_gens.append(genkey)
        if pgen == 0.0:
            zero_gens.append(genkey)
        if genbus not in genidx_dict:
            genidx_dict.update({genbus: [idx]})
        else:
            genidx_dict[genbus].append(idx)
        if genbus not in plant_dict:
            if pgen > 0.0:
                plant_dict.update({genbus: [[pgen, genkey]]})
        else:
            if pgen > 0.0:
                plant_dict[genbus].append([pgen, genkey])

    tempdict = {}
    for gbus in plant_dict:
        if len(plant_dict[gbus]) > 1:
            plant_dict[gbus].sort(reverse=True)
            tempdict.update({gbus: [x[1] for x in plant_dict[gbus]]})
    plant_dict = tempdict

    # == ADD FIXED SHUNT DATA TO NETWORK ==========================================================
    # fixshunt = (fxshunt.i, fxshunt.id, fxshunt.status, fxshunt.gl, fxshunt.bl)
    fxshidx_dict = {}
    if raw_data.raw.fixed_shunts.values():
        print('ADD FIXED SHUNTS ...................................................')
        for fxshunt in raw_data.raw.fixed_shunts.values():
            status = bool(fxshunt.status)
            if not status:
                continue
            shuntbus = fxshunt.i
            shuntname = str(shuntbus) + '-FX'
            nomkv = busnomkv_dict[shuntbus]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_shunt(net_a, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            if UseNetC:
                pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxshidx_dict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
    swsh_keyidx = {}
    swsh_idxkey = {}
    swshbus_dict = {}
    swshkey_dict = {}
    area_swhunts = {}
    for area in areas:
        area_swhunts.update({area: []})
    if raw_data.raw.switched_shunts.values():
        print('ADD SWITCHED SHUNTS ................................................')
        for swshunt in raw_data.raw.switched_shunts.values():
            status = bool(swshunt.stat)
            if not status:
                continue
            shuntbus = swshunt.i
            swshkey = str(shuntbus) + '-SW'
            vreg = buskv_dict[shuntbus]
            nomkv = busnomkv_dict[shuntbus]
            if shuntbus in genbuses:
                gidx = genidx_dict[shuntbus][0]
                if net_a.gen.loc[gidx, 'in_service']:
                    vreg = net_a.gen.loc[gidx, 'vm_pu']
            steps = [swshunt.n1, swshunt.n2, swshunt.n3, swshunt.n4, swshunt.n5, swshunt.n6, swshunt.n7, swshunt.n8]
            mvars = [swshunt.b1, swshunt.b2, swshunt.b3, swshunt.b4, swshunt.b5, swshunt.b6, swshunt.b7, swshunt.b8]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(mvars)):
                if mvars[j] < 0.0:
                    total_qmin += steps[j] * mvars[j]
                elif mvars[j] > 0.0:
                    total_qmax += steps[j] * mvars[j]
            pgen = 0.0
            pmax = 0.0
            pmin = 0.0
            shuntmva = math.sqrt(pmax ** 2 + total_qmax ** 2)
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, shuntbus, pgen, vm_pu=vreg, sn_mva=shuntmva, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=total_qmax, min_q_mvar=total_qmin,
                                rdss_pu=99.99, xdss_pu=99.99, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            if UseNetC:
                pp.create_gen(net_c, shuntbus, pgen, vm_pu=vreg, sn_mva=shuntmva, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=total_qmax, min_q_mvar=total_qmin,
                              rdss_pu=99.9, xdss_pu=99.99, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH', index=idx)
            swshidx_dict.update({shuntbus: idx})
            swshidxs.append(idx)
            swsh_keyidx.update({swshkey: idx})
            swsh_idxkey.update({idx: swshkey})
            swshbus_dict.update({idx: shuntbus})
            swshkey_dict.update({shuntbus: swshkey})
            area_swhunts[busarea_dict[shuntbus]].append(swshkey)

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st, line.len, line.met)
    line_keyidx = {}
    lineidxs = []
    # branch_areas = {}
    zero_branches = []
    print('ADD LINES ..........................................................')
    for line in raw_data.raw.nontransformer_branches.values():
        frombus = line.i
        tobus = line.j
        ckt = line.ckt
        linekey = str(frombus) + '-' + str(tobus) + '-' + ckt
        status = bool(line.st)
        length = line.len
        if length == 0.0:
            length = 1.0
        kv = busnomkv_dict[frombus]
        zbase = kv ** 2 / mva_base
        r_pu = line.r / length
        x_pu = line.x / length
        b_pu = line.b / length
        r = r_pu * zbase
        x = x_pu * zbase
        b = b_pu / zbase
        capacitance = 1e9 * b / (2 * math.pi * 60.0)
        base_mva_rating = line.ratea
        mva_rating = line.ratec
        i_rating_a = base_mva_rating / (math.sqrt(3) * kv)
        i_rating_c = mva_rating / (math.sqrt(3) * kv)
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey, max_loading_percent=MaxLoading, in_service=status)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        if UseNetC:
            pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey, max_loading_percent=MaxLoading, in_service=status, index=idx)
        line_keyidx.update({linekey: idx})
        lineidxs.append(idx)
        # branch_areas.update({linekey: [busarea_dict[frombus], busarea_dict[tobus]]})
        if not status:
            zero_branches.append(linekey)

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.nomv1,
    #           xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.nomv2, xfmr.stat)
    xfmr_keyidx = {}
    xfmr_ratea_dict = {}
    xfmr_ratec_dict = {}
    xfmridxs = []
    print('ADD 2W TRANSFORMERS ................................................')
    for xfmr in raw_data.raw.transformers.values():
        status = bool(xfmr.stat)                                                # XFMR STATUS (TRUE = IN-SERVICE)
        xfmrkey = str(xfmr.i) + '-' + str(xfmr.j) + '-' + xfmr.ckt              # DEFINE XFMR KEY
        wind1 = xfmr.i                                                          # GET BUS CONNECTED TO WINDING1
        wind2 = xfmr.j                                                          # GET BUS CONNECTED TO WINDING2
        lowbus = wind1                                                          # ASSUME LOWBUS CONNECTED TO WINDING1
        lowkv = busnomkv_dict[wind1]                                            # GET KV OF ASSUMED LOWBUS
        lv_tap = xfmr.windv1                                                    # GET ASSUMED LOWVOLTAGE NLTC
        highbus = wind2                                                         # ASSUME HIGHBUS CONNECTED WINDING2
        highkv = busnomkv_dict[wind2]                                           # GET KV OF ASSUMED HIGHBUS
        hv_tap = xfmr.windv2                                                    # GET ASSUMED HIGHVOLTAGE NLTC
        tapside = 'lv'                                                          # ASSIGN NLTC TO LOWSIDE
        net_tap = lv_tap / hv_tap                                               # NET TAP SETTING ON LOWSIDE
        if lowkv > highkv:                                                      # IF WINDING1 IS CONNECTED TO HIGHBUS...
            highbus, lowbus = lowbus, highbus                                   # SWAP HIGHBUS, LOWBUS
            highkv, lowkv = lowkv, highkv                                       # SWAP HIGHKV, LOWKV
            hv_tap, lv_tap = lv_tap, hv_tap                                     # SWAP HIGHVOLTAGE NLTC, LOWVOLTAGE NLTC
            tapside = 'hv'                                                      # ASSIGN NLTC TO HIGHSIDE
            net_tap = hv_tap / lv_tap                                           # NET TAP SETTING ON HIGHSIDE
        r_pu_sbase = xfmr.r12                                                   # RPU @ MVA_BASE (FROM RAW DATA)
        x_pu_sbase = xfmr.x12                                                   # XPU @ MVA_BASE (FROM RAW DATA)
        # -- RATE A 'NAMEPLATE' IMPEDANCE -------------------------------------
        r_pu_a = r_pu_sbase * xfmr.rata1 / mva_base                             # PANDAPOWER USES RATING AS TEST MVA
        x_pu_a = x_pu_sbase * xfmr.rata1 / mva_base                             # SO CONVERT TO RATE A BASE
        z_pu_a = math.sqrt(r_pu_a ** 2 + x_pu_a ** 2)                           # CALCULATE RATE A 'NAMEPLATE' PU IMPEDANCE
        z_pct_a = 100.0 * z_pu_a                                                # PERCENT IMPEDANCE (FOR PANDAPOWER XFMR)
        r_pct_a = 100.0 * r_pu_a                                                # PERCENT RESISTANCE  (FOR PANDAPOWER XFMR)
        # -- RATE C 'NAMEPLATE' IMPEDANCE -------------------------------------
        r_pu_c = r_pu_sbase * xfmr.ratc1 / mva_base                             # PANDAPOWER USES RATING AS TEST MVA
        x_pu_c = x_pu_sbase * xfmr.ratc1 / mva_base                             # SO CONVERT TO RATE C BASE
        z_pu_c = math.sqrt(r_pu_c ** 2 + x_pu_c ** 2)                           # CALCULATE RATE C 'NAMEPLATE' PU IMPEDANCE
        z_pct_c = 100.0 * z_pu_c                                                # PERCENT IMPEDANCE (FOR PANDAPOWER XFMR)
        r_pct_c = 100.0 * r_pu_c                                                # PERCENT RESISTANCE  (FOR PANDAPOWER XFMR)

        shuntname = str(highbus) + '-FXMAG'
        fx_p = 0.0
        fx_q = 0.0
        if xfmr.mag1 != 0.0 or xfmr.mag2 != 0.0:
            fx_p = mva_base * xfmr.mag1
            fx_q = -mva_base * xfmr.mag2
            # -- BASE NETWORK MAGNETIZING ADMITTANCE ----------------------------------------------
            idx = pp.create_shunt(net_a, wind1, q_mvar=fx_q, p_mw=fx_p, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK MAGNETIZING ADMITTANCE --------------------------------------------------
            if UseNetC:
                pp.create_shunt(net_c, wind1, q_mvar=fx_q, p_mw=fx_p, step=1, max_step=True, name=shuntname, index=idx)
            fxshidx_dict.update({wind1: idx})

        # -- TAP SETTINGS -------------------------------------------------------------------------
        tapmax = 2
        tapneutral = 0
        tapmin = -2
        tapsteppct = 100.0 * abs(1 - net_tap)
        if net_tap > 1.0:
            tappos = 1
        elif net_tap == 1.0:
            tappos = 0
        elif net_tap < 1.0:
            tappos = -1

        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_transformer_from_parameters(net_a, highbus, lowbus, xfmr.rata1, highkv, lowkv, r_pct_a, z_pct_a, pfe_kw=0.0, i0_percent=0.0,
                                                    shift_degree=xfmr.ang1, tap_side=tapside, tap_neutral=tapneutral, tap_max=tapmax, tap_min=tapmin,
                                                    tap_step_percent=tapsteppct, tap_pos=tappos,
                                                    in_service=status, name=xfmrkey, max_loading_percent=MaxLoading)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        if UseNetC:
            pp.create_transformer_from_parameters(net_c, highbus, lowbus, xfmr.ratc1, highkv, lowkv, r_pct_c, z_pct_c, pfe_kw=0.0, i0_percent=0.0,
                                                  shift_degree=xfmr.ang1, tap_side=tapside, tap_neutral=tapneutral, tap_max=tapmax, tap_min=tapmin,
                                                  tap_step_percent=tapsteppct, tap_pos=tappos,
                                                  in_service=status, name=xfmrkey, max_loading_percent=MaxLoading, index=idx)

        xfmr_keyidx.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmr_ratec_dict.update({xfmrkey: xfmr.ratc1})
        xfmridxs.append(idx)
        # branch_areas.update({xfmrkey: [busarea_dict[highbus], busarea_dict[lowbus]]})
        if not status:
            zero_branches.append(xfmrkey)

    # == ADD EXTERNAL GRID ========================================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)                                                 # CURRENT RATING USING SWING KV
    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.011, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, type='p')
    # pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=1, cq0_eur=1e6, type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    if UseNetC:
        pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
        tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', in_service=True, max_loading_percent=100.0)
        pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                           s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.01, rx_min=0.01, index=ext_grid_idx)
        pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, cq1_eur_per_mvar=0, cq0_eur=1e9)
        # pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=0, cq0_eur=1e9, type='q')

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 3), 'sec')
    # =============================================================================================
    # -- NETWORKS CREATED -------------------------------------------------------------------------
    # =============================================================================================

    # -- SOLVE NETWORK WITH POWERFLOW -------------------------------------------------------------
    solve_starttime = time.time()                                                                   # INITIALIZE START-TIME
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN POWERFLOW ON THIS NETWORK
    print('   POWERFLOW SOLVED ................................................', round(time.time() - solve_starttime, 3), 'sec')

    # -- SOLVE NETWORK WITH OPF -------------------------------------------------------------------
    solve_starttime = time.time()                                                                   # INITIALIZE START-TIME
    net = copy.deepcopy(net_a)                                                                      # COPY NETWORK A
    pp.runopp(net, init='pf')                                                                                            # RUN OPF ON THIS NETWORK
    net_a = copy_opf_to_network(net, net_a, gen_keyidx, genbus_dict, swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx)  # COPY OPF RESULTS TO THIS NETWORK
    pp.runpp(net_a, enforce_q_lims=True)                                                                                 # RUN POWERFLOW
    if UseNetC:
        net_c = copy_opf_to_network(net, net_c, gen_keyidx, genbus_dict, swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx)  # COPY OPF RESULTS TO THIS NETWORK
        pp.runpp(net_c, enforce_q_lims=True)                                                                                 # RUN POWERFLOW
    opf_solvetime = time.time() - solve_starttime
    print('   OPTIMAL POWERFLOW SOLVED ........................................', round(opf_solvetime, 3), 'sec')

    # =============================================================================================
    # -- TRY TO FILTER OUT SOME GENERATOR AND BRANCH OUTAGES  -------------------------------------
    # =============================================================================================
    NumBuses = len(busidxs)
    goutage_keys = list(outage_dict['gen'].keys())                                                   # GET OUTAGED GENERATOR KEYS
    boutage_keys = list(outage_dict['branch'].keys())                                                # GET OUTAGED BRANCH KEYS
    NumOutages_0 = len(goutage_keys) + len(boutage_keys)

    goutage_keys = [x for x in goutage_keys if x not in zero_gens]                                  # REMOVE ANY GENERATOR OUTAGES WITH PGEN=0.0 or OFF-LINE
    boutage_keys = [x for x in boutage_keys if x not in zero_branches]                              # REMOVE ANY BRANCH OUTAGES if OPEN

    # -- OUTAGE ONLY THE LARGEST GENERATOR IN A PLANT -------------------------
    for bkey in plant_dict:                                                                         # LOOP ACROSS GENERATORS ON THE SAME BUS
        for gkey in plant_dict[bkey]:                                                               # GET THE KEY OF GENERATOR
            if gkey in goutage_keys:                                                                # CHECK IF THIS GENERATOR IS IN THE GENERATOR OUTAGES...
                gkey_index = plant_dict[bkey].index(gkey)                                           # IF SO, GET THE INDEX OF THE SORTED PLANT GENERATORS
                delete_keys = plant_dict[bkey][gkey_index + 1:]                                     # SLICE THE LIST TO INCLUDE GENERATORS WITH LESS PGEN
                if delete_keys:                                                                     # IF THE LESSER GENERATOR LIST IS NOT EMPTY...
                    goutage_keys = [x for x in goutage_keys if x not in delete_keys]                # REMOVE THE LESSER GENERATORS ON SAME BUS FROM CONTINGENCY LIST
                break                                                                               # GET THE NEXT GROUP OF GENERATORS ON THE SAME BUS

    # -- ORDER GENERATOR OUTAGES LARGEST TO SMALLEST PGEN ---------------------
    gen_pgens = []
    for gkey in goutage_keys:
        gidx = gen_keyidx[gkey]
        pgen = net_a.gen.loc[gidx, 'p_mw']
        gen_pgens.append([pgen, gkey])
    gen_pgens.sort(reverse=True)
    sorted_gkeys = [x[1] for x in gen_pgens]
    goutage_keys = [x for x in sorted_gkeys]

    # -- ORDER BRANCH OUTAGES LARGEST TO SMALLEST FLOW ------------------------
    branch_flows = []
    for bkey in boutage_keys:
        if bkey in line_keyidx:
            lidx = line_keyidx[bkey]
            branch_flow = net_a.res_line.loc[lidx, 'p_from_mw']
            branch_flows.append([abs(branch_flow), bkey])
        elif bkey in xfmr_keyidx:
            xidx = xfmr_keyidx[bkey]
            branch_flow = net_a.res_trafo.loc[xidx, 'p_hv_mw']
            branch_flows.append([abs(branch_flow), bkey])
    branch_flows.sort(reverse=True)
    sorted_bkeys = [x[1] for x in branch_flows]
    boutage_keys = [x for x in sorted_bkeys]

    goutage_keys = goutage_keys[:100]                                           # SET HOW MANY GENERATOR OUTAGES TO CONSIDER
    boutage_keys = boutage_keys[:100]                                           # SET HOW MANY BRANCH OUTAGES TO CONSIDER
    NumOutages_1 = len(goutage_keys) + len(boutage_keys)
    print('   BUSES ...........................................................', NumBuses)
    print('   OUTAGES .........................................................', NumOutages_0, '-', NumOutages_1)

    # *********************************************************************************************
    # -- FIND BASECASE OPF OPERATING POINT --------------------------------------------------------
    # *********************************************************************************************
    print('-------------------- ATTEMPTING BASECASE SCOPF ---------------------')

    if UseNetC:                                                                                     # IF USING NET_C FOR SCOPF...
        c_net = copy.deepcopy(net_c)                                                                # GET COPY OF THE RATEC NETWORK
        c_net.bus['min_vm_pu'] = net_a.bus['min_vm_pu']                                             # CHANGE MIN BUS VOLTAGE
        c_net.bus['max_vm_pu'] = net_a.bus['max_vm_pu']                                             # CHANGE MAX BUS VOLTAGE
    else:                                                                                           # IF USING NET_A...
        c_net = copy.deepcopy(net_a)                                                                # GET COPY OF THE RATEA NETWORK

    # -- GET DISTRIBUTION FACTOR DICTIONARY -------------------------------------------------------
    # ScoringMethod = 2                                                                               # TODO DEVELOPMENT
    # MaxRunningTime = 2700                                                                           # TODO DEVELOPMENT
    # if ScoringMethod in [1, 3]:                                                                     # CHECK IF REAL-TIME SCORING METHOD
    #     dfstart_time = time.time()
    #     net = copy.deepcopy(c_net)
    #     df_dict = get_df_dict(net, goutage_keys, boutage_keys, gen_keyidx, line_keyidx, xfmr_keyidx, load_keyidx, total_loadp)      # df_dict{okey1:{bkey1:df, ...},okey2:{bkey1:df, ...}, ...}
    #     print('DISTRIBUTION FACTORS CALCULATED ....................................', round(time.time() - dfstart_time, 3), 'sec')

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    # -- LOOP WHILE THERE ARE REMAINING DOMINANT OUTAGES ------------------------------------------
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    scopf_start_time = time.time()                                                                  # SET THE WHILE LOOP START TIME
    step = 0                                                                                        # INITIALIZE WHILE LOOP ITERATOR
    processed_outages = []                                                                          # INITIALIZE LIST OF ALREADY PROCESSED OUTAGES

    last_iteration_time = 0.0                                                                       # INITIALIZE TIME FOR EACH WHILE LOOP ITERATION
    time_per_outage = opf_solvetime
    elapsed_time = round(time.time() - master_start_time, 3)                                        # GET THE ELAPSED TIME SO FAR
    time_to_finalize = opf_solvetime + 10.0
    countdown_time = MaxRunningTime - elapsed_time - time_to_finalize                               # INITIALIZE COUNTDOWN TIME

    outage_keys = goutage_keys + boutage_keys

    while countdown_time > 0.0:                                                                     # LOOP WHILE TIME REMAINS
        start_iteration_time = time.time()                                                          # INITIALIZE START ITERATION TIME
        pp.runpp(c_net, enforce_q_lims=True)                                                        # SOLVE THIS MASTER BASECASE
        base_pgen_dict = get_generator_pgens(c_net, online_gens, gen_keyidx)                        # GET GENERATORS PGEN FOR THIS MASTER BASECASE

        # == GET DOMINANT OUTAGES RESULTING IN BRANCH LOADING VIOLATIONS ==========================
        dominant_outages, num_overloads = get_dominant_outages_ac(c_net, outage_keys, online_gens, gen_keyidx,            #
                                                                  line_keyidx, xfmr_keyidx, load_keyidx,                  #
                                                                  total_loadp, step)                                      # GET DOMINANT GENERATOR AND BRANCH OUTAGES USING AC POWERFLOW
        # if ScoringMethod in [1, 3]:
        #     dominant_outages, num_overloads = get_dominant_outages_df(c_net, outage_keys, online_gens, gen_keyidx,      # CHECK IF REAL-TIME SCORING METHOD
        #                                                               line_keyidx, xfmr_keyidx, df_dict, step)          # GET DOMINANT GENERATOR AND BRANCH OUTAGES USING DF
        # elif ScoringMethod in [2, 4]:                                                                                   # CHECK IF OFF-LINE SCORING METHOD
        #     dominant_outages, num_overloads = get_dominant_outages_ac(c_net, outage_keys, online_gens, gen_keyidx,      #
        #                                                               line_keyidx, xfmr_keyidx, load_keyidx,            #
        #                                                               total_loadp, step)                                # GET DOMINANT GENERATOR AND BRANCH OUTAGES USING AC POWERFLOW

        o_keys = dominant_outages[:num_overloads]                                                                       # SET HOW MANY DOMINANT OUTAGES TO PROCESS
        print('{0:<2d} PROCESSED OUTAGES'.format(step), processed_outages)                                              # PRINT STATEMENT
        print('{0:<2d}    NOW PROCESSING'.format(step), o_keys, '.....', round(countdown_time, 1), 'Sec remaining')     # PRINT STATEMENT

        if num_overloads == 0:                                                                                          # CHECK IF NO OVERLOADS FOUND...
            print()
            break                                                                                                       # BREAK OUT OF WHILE LOOP
        if countdown_time < (time_per_outage * num_overloads):
            num_overloads = int(countdown_time / time_per_outage)
            if num_overloads < 1:
                break

        # ==========================================================================================
        # == RUN THE DOMINANT OUTAGES ON THIS BASECASE WITH OPF ====================================
        # ==========================================================================================
        opf_pgen_dict = {}                                                                          # DECLARE OPF GENERATOR DICT
        gen_minmax_dict = {}                                                                        # DECLARE GENERATOR MIN-MAX CHANGE DICT
        gdelta_threshold = 1.2                                                                      # SET HOW MUCH GENERATOR CHANGE TO CONSIDER

        # -- RUN THE OUTAGES IN PARLLEL ---------------------------------------
        arglist = [[c_net, x, participating_gens, gen_keyidx, line_keyidx, xfmr_keyidx, genbus_dict,            #
                    swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx] for x in o_keys]                         # GET ARGUMENT LIST FOR EACH PROCESS
        results = parallel_run_outage_opf(arglist)                                                              # GET PARALLEL RESULTS
        for opf_pgen_dict_i in results:                                                                         # LOOP ACROSS THE OUTAGES OPF GENERATOR DICTS
            opf_pgen_dict.update(opf_pgen_dict_i)                                                               # UPDATE THE MASTER OPF GENERATOR DICT

        # Parallel_Process_Outages = True                                                                         # SET PARALLEL PROCESSING FLAG
        # if not Parallel_Process_Outages:                                                                        # IF NOT PARALLEL PROCESSING...
        #     for o_key in o_keys:                                                                                # LOOP THROUGH OUTAGE KEYS
        #         opf_pgen_dict_i = run_outage_opf(c_net, o_key, participating_gens, gen_keyidx, line_keyidx,     #
        #                                          xfmr_keyidx, genbus_dict, swingbus, swsh_keyidx,               # GET THIS OUTAGE OPF GENERATOR DICT
        #                                          swshbus_dict, ext_grid_idx)                                    # {O_KEY1:{GKEY1:PGEN1, GKEY2:PGEN2, ...}}
        #         opf_pgen_dict.update(opf_pgen_dict_i)                                                           # UPDATE THE MASTER OPF GENERATOR DICT
        #
        # if Parallel_Process_Outages:                                                                            # IF PARALLEL PROCESSING...
        #     arglist = [[c_net, x, participating_gens, gen_keyidx, line_keyidx, xfmr_keyidx, genbus_dict,        #
        #                 swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx] for x in o_keys]                     # GET ARGUMENT LIST FOR EACH PROCESS
        #     results = parallel_run_outage_opf(arglist)                                                          # GET PARALLEL RESULTS
        #     for opf_pgen_dict_i in results:                                                                     # LOOP ACROSS THE OUTAGES OPF GENERATOR DICTS
        #         opf_pgen_dict.update(opf_pgen_dict_i)                                                           # UPDATE THE MASTER OPF GENERATOR DICT

        # -- INITIALIZE MIN-MAX GENERATOR CHANGE DICT -------------------------
        for g_key in participating_gens:                                                            # LOOP ACROSS PARTICIPATING GENERATORS
            g_idx = gen_keyidx[g_key]                                                               # GET GENERATOR INDEX
            base_pgen = base_pgen_dict[g_key]                                                       # GET THIS GENERATOR'S BASECASE PGEN
            gen_minmax_dict.update({g_key: [0.0, 0.0, 0.0, base_pgen, g_idx]})                      # INITIALIZE GENERATOR MIN-MAX CHANGE DICT

        # == DETERMINE HOW MUCH THE PARTICIPATING GENERATORS CHANGED ==========
        for o_key in opf_pgen_dict:                                                                 # LOOP THROUGH THE DOMINANT OUTAGES FROM OPF RESULTS
            for g_key in opf_pgen_dict[o_key]:                                                      # LOOP ACROSS THE ONLINE GENERATORS
                if g_key == o_key:                                                                  # CHECK IF THIS OUTAGE IS THIS GENERATOR...
                    continue                                                                        # IF SO... GET THE NEXT GENERATOR
                base_pgen = gen_minmax_dict[g_key][3]                                               # GET THIS GENERATOR'S BASECASE PGEN
                pgen = opf_pgen_dict[o_key][g_key]                                                  # GET THIS GENERATOR'S N-1 OPF PGEN
                gdelta = pgen - base_pgen                                                           # CALCULATE PARTICIPATING GENERATOR CHANGE (COMPARED TO BASECASE)
                if gdelta < gen_minmax_dict[g_key][1]:                                              # IF THIS GENERATOR DECREASE MORE THAN LARGEST DECREASE...
                    gen_minmax_dict[g_key][1] = gdelta                                              # SET THIS GENERATORS LARGEST DECREASE
                if gdelta > gen_minmax_dict[g_key][2]:                                              # IF THIS GENERATOR INCREASED MORE THAN LARGEST INCREASE...
                    gen_minmax_dict[g_key][2] = gdelta                                              # SET THIS GENERATORS LARGEST INCREASE
                gdelta_neg = gen_minmax_dict[g_key][1]                                              # GET THIS LOOPS GENERATION DECREASE
                gdelta_pos = gen_minmax_dict[g_key][2]                                              # GET THIS LOOPS GENERATION INCREASE
                gen_minmax_dict[g_key][0] = max(-gdelta_neg, gdelta_pos)                            # SET SORTING VALUE TO MAX OF -DECREASE OR INCREASE

        gdelta_data = []                                                                            # INITIALIZE LIST FOR SORTING GENERATOR CHANGE DATA
        for g_key in gen_minmax_dict:                                                               # LOOP THROUGH GEN MIN-MAX CHANGE DICT
            gdelta_data.append(gen_minmax_dict[g_key])                                              # ADD GEN MIN-MAX DATA TO LIST
        gdelta_data.sort(reverse=True)                                                              # SORT LARGEST TO SMALLEST PGEN CHANGE
        gdelta_data = [x for x in gdelta_data if x[0] > gdelta_threshold]                           # REMOVE GENERATORS WITH INSIGNIFICANT CHANGE
        gdelta_data = [x[1:] for x in gdelta_data]                                                  # REMOVE GDELTAS SORT VALUE

        for data in gdelta_data:                                                                    # LOOP ACROSS THE GENERATOR CHANGE DATA
            gdelta_neg = data[0]                                                                    # GET HOW MUCH THIS GENERATOR WENT DOWN (FOR THE OUTAGES RUN)
            gdelta_pos = data[1]                                                                    # GET HOW MUCH THIS GENERATOR WENT UP (FOR THE OUTAGES RUN)
            base_pgen = data[2]                                                                     # GET THIS GENERATORS PRE-OUTAGE PGEN
            g_idx = data[3]                                                                         # GET THIS GENERATORS INDEX
            # -- CHECK FOR PGEN CHANGE IN BOTH DIRECTIONS ---------------------
            if gdelta_neg < -gdelta_threshold and gdelta_pos > gdelta_threshold:                    # CHECK IF THIS GENERATOR BOTH DECREASED AND INCREASED PGEN
                pgen_min = base_pgen + gdelta_neg                                                   # CALCULATE THE MINIMUM PGEN
                pgen_max = base_pgen + gdelta_pos                                                   # CALCULATE THE MAXIMUM PGEN
                pgen = base_pgen + (gdelta_neg + gdelta_pos) / 2                                    # CALCULATE THE AVERAGE PGEN
                # -- SET GENERATOR PGEN AND PMIN-PMAX -------------------------
                c_net.gen.loc[g_idx, 'p_mw'] = pgen                                                 # SET THIS GENERATORS PGEN
                c_net.gen.loc[g_idx, 'min_p_mw'] = pgen_min                                         # SET THIS GENERATORS MINPGEN
                c_net.gen.loc[g_idx, 'max_p_mw'] = pgen_max                                         # SET THIS GENERATORS MAXPGEN
                # print('DOWN AND UP')

            # -- CHECK FOR PGEN CHANGE DOWN ONLY  -----------------------------
            elif gdelta_neg < -gdelta_threshold and gdelta_pos < gdelta_threshold:                  # CHECK IF THIS GENERATOR ONLY DECREASED PGEN
                pgen_max = base_pgen + gdelta_neg                                                   # CALCULATE MAXPGEN
                pgen = pgen_max                                                                     # ASSIGN PGEN = MAXPGEN
                # -- SET GENERATOR PGEN AND PMAX ------------------------------
                c_net.gen.loc[g_idx, 'p_mw'] = pgen                                                 # SET THIS GENERATORS PGEN
                c_net.gen.loc[g_idx, 'max_p_mw'] = pgen_max                                         # SET THIS GENERATORS MAXPGEN
                # print('DOWN')

            # -- CHECK FOR PGEN CHANGE UP ONLY  -------------------------------
            elif gdelta_neg > -gdelta_threshold and gdelta_pos > gdelta_threshold:                  # CHECK IF THIS GENERATOR ONLY INCREASED PGEN
                pgen_min = base_pgen + gdelta_pos                                                   # CALCULATE MINPGEN
                pgen = pgen_min                                                                     # ASSIGN PGEN = MINPGEN
                # -- SET GENERATOR PGEN AND PMIN ------------------------------
                c_net.gen.loc[g_idx, 'p_mw'] = pgen                                                 # SET THIS GENERATORS PGEN
                c_net.gen.loc[g_idx, 'min_p_mw'] = pgen_min                                         # SET THIS GENERATORS MINPGEN
                # print('UP')

        step += 1                                                                                   # INCREMENT ITERATOR
        processed_outages += o_keys                                                                 # UPDATE THE PROCESSED OUTAGES
        last_iteration_time = time.time() - start_iteration_time                                    # CALCULATE THIS ITERATION TIME
        countdown_time -= last_iteration_time                                                       # DECREMENT COUNTDOWN TIME
        time_per_outage = last_iteration_time / len(o_keys)                                         # CALCULATE TIME PER OUTAGE

        outage_keys = dominant_outages[1:]                                                          # UPDATE OUTAGE KEYS (REMOVE THIS LOOPS WORST DOMINANT OUTAGE)
        # outage_keys = dominant_outages[0:]                                                          # UPDATE OUTAGE KEYS (USE ALL DOMINANT OUTAGES)
        # outage_keys = dominant_outages[num_overloads:]                                              # UPDATE OUTAGE KEYS (REMOVE THIS LOOPS DOMINANT OUTAGES)

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    # -- END OF WHILE LOOP ------------------------------------------------------------------------
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

    # =============================================================================================
    # -- FINALIZE THE SCOPF BASECASE --------------------------------------------------------------
    # =============================================================================================
    print('---------------- RUNNING OPF ON FINAL SCOPF BASECASE ---------------')           # PRINT MESSAGE
    if num_overloads > 0:                                                                   # IF NOT ALL OVERLOADS PROCESSED...
        print('ITERATIONS TIMED OUT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')       # PRINT MESSAGE
    a_net = copy.deepcopy(c_net)                                                            # COPY LAST OPF BASECASE NETWORK TO FINAL SCOPF BASECASE

    if UseNetC:                                                                             # IF USING RATEC...
        a_net.line['max_i_ka'] = net_a.line['max_i_ka']                                     # CHANGE LINE MAXLOADING TO RATEA
        a_net.trafo['sn_mva'] = net_a.trafo['sn_mva']                                       # CHANGE XFMR MAXLOADING TO RATEA
        a_net.bus['min_vm_pu'] = net_a.bus['min_vm_pu']                                     # CHANGE MIN BUS VOLTAGE
        a_net.bus['max_vm_pu'] = net_a.bus['max_vm_pu']                                     # CHANGE MAX BUS VOLTAGE
    pp.runpp(a_net, enforce_q_lims=True)                                                    # SOLVE THIS FINAL BASECASE
    pp.runopp(a_net, init='pf')                                                                                            # RUN OPF ON THIS NETWORK
    net_a = copy_opf_to_network(a_net, net_a, gen_keyidx, genbus_dict, swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx)  # <---- IS THE FINAL SCOPF BASECASE

    pp.runpp(net_a, enforce_q_lims=True)                                                                                   # SOLVE THIS FINAL BASECASE
    min_busvoltage, max_busvoltage = get_minmax_voltage(net_a)                              # GET MIN-MAX BASECASE BUS VOLTAGES
    min_busvoltage = round(min_busvoltage, 5)                                               # FORMAT MIN VOLTAGE
    max_busvoltage = round(max_busvoltage, 5)                                               # FORMAT MAX VOLTAGE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                  # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                # GET EXTERNAL GRID REACTIVE POWER
    max_basecase_loading = get_maxloading(net_a, line_keyidx, xfmr_keyidx)                  # GET MAX BASECASE BRANCH LOADING
    print('MAX BASECASE LOADING =', max_basecase_loading)                                           # PRINT MAX BASECASE BRANCH LOADING
    print('MIN - MAX BASECASE VOLTAGE =', [round(min_busvoltage, 5), round(max_busvoltage, 5)])     # PRINT MIN-MAX BASECASE VOLTAGES
    print('EXT_PGEN - EXT_QGEN =', [round(ex_pgen, 4), round(ex_qgen, 4)])                          # PRINT EXTERNAL GRID REAL AND REACTIVE POWER
    print('BASECASE SCOPF NETWORK CREATED .....................................', round(time.time() - scopf_start_time, 3), 'sec')
    # break  # EXIT WHILE LOOP AND FINE-TUNE BASECASE

    # =============================================================================================
    # -- FINE-TUNE FINAL SCOPF BASECASE -----------------------------------------------------------
    # =============================================================================================
    print('-------------------- FINE-TUNING SCOPF BASECASE --------------------')
    start_time = time.time()

    # -- MAKE SHURE GENERATORS WITH PMAX=0 HAVE PGEN=0 --------------------------------------------
    for gkey in gen_keyidx:                                                                         # LOOP ACROSS GENERATOR KEYS
        gidx = gen_keyidx[gkey]                                                                    # GET GENERATOR INDEX
        if net_a.gen.loc[gidx, 'max_p_mw'] == 0.0:                                                  # CHECK IF PMAX=0
            net_a.gen.loc[gidx, 'p_mw'] = 0.0                                                       # SET PGEN=0

    # -- INSURE GENERATORS ARE MEETING VOLTAGE SCHEDULE -------------------------------------------
    for genbus in genidx_dict:
        gen_off_schedule = False                                                                    # RESET GENERATOR OFF SCHEDULE FLAG
        bus_voltage = None                                                                          # INITIALIZE BUS VOLTAGE
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        gen_indexes = genidx_dict[genbus]                                                           # GET GENERATOR INDEX
        for gidx in gen_indexes:                                                                    # LOOP ACROSS GENERATORS ON THIS BUS
            qgen = net_a.res_gen.loc[gidx, 'q_mvar']                                                # THIS GENERATORS QGEN
            qmin = net_a.gen.loc[gidx, 'min_q_mvar']                                                # THIS GENERATORS QMIN
            qmax = net_a.gen.loc[gidx, 'max_q_mvar']                                                # THIS GENERATORS QMAX
            gvreg = net_a.res_gen.loc[gidx, 'vm_pu']
            bus_voltage = net_a.res_bus.loc[genbus, 'vm_pu']                                        # GET THIS GENERATORS BUS VOLTAGE
            if qgen == qmin or qgen == qmax or gvreg != bus_voltage:                                # IF THIS GENERATOR AT +/- QLIMIT OR NO MEETING VREG
                gen_off_schedule = True                                                             # SET OFF SCHEDULE FLAG = TRUE
                break                                                                               # BREAK AND LOOP ACROSS THIS BUSES GENERATORS
        if gen_off_schedule:                                                                        # GENERATOR FOUND OFF SCHEDULE...
            for gidx in gen_indexes:                                                                # LOOP ACROSS GENERATORS ON THIS BUS
                net_a.gen.loc[gidx, 'vm_pu'] = bus_voltage                                          # THIS NETWORK, SET THIS GENERATORS VREG TO BUS VOLTAGE
                # print('Generator at Bus', genbus, 'at Qmax')
            if genbus in swshidx_dict:                                                              # CHECK IF THERE IS A SWSHUNT ON THIS GEN BUS
                shidx = swshidx_dict[genbus]                                                        # GET SWSHUNT INDEX
                net_a.gen.loc[shidx, 'vm_pu'] = bus_voltage                                         # ALSO SET SWSHUNT VREG TO BUS VOLTAGE
                # print('Generator(s) and SWShunt on Same Bus', genbus)
            pp.runpp(net_a, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW

    # -- INSURE SWSHUNTS SUSCEPTANCE IS WITHIN LIMITS IN BASECASE ---------------------------------
    # -- HOPE CONSERVATIVE ENOUGH TO HOLD UP WITH CONTINGENCIES -----------------------------------
    for shkey in swsh_keyidx:                                                                       # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_keyidx[shkey]                                                                  # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        qgen = net_a.res_gen.loc[shidx, 'q_mvar']                                                   # GET SWSHUNT QGEN
        qmin = net_a.gen.loc[shidx, 'min_q_mvar']                                                   # GET MINIMUM SWSHUNT REACTIVE CAPABILITY
        qmax = net_a.gen.loc[shidx, 'max_q_mvar']                                                   # GET MAXIMUM SWSHUNT REACTIVE CAPABILITY
        voltage = net_a.res_bus.loc[shbus, 'vm_pu']                                                 # GET SWSHUNT BUS VOLTAGE
        # if qgen > qmax:
        #     print('Shunt with QGen > QMax', shkey, qgen, qmax)
        # if qgen < qmin:
        #     print('Shunt with QGen < QMin', shkey, qgen, qmin)
        if voltage < 1.0:                                                                           # IF BUS VOLTAGE IS < 1.0 (SUSCEPTANCE COULD BE EXCEEDED)
            if qgen / voltage ** 2 < 0.98 * qmin < 0.0:                                             # CHECK IF QMIN IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmin = min(qmax, 0.99 * qmin * voltage ** 2)                                    # CALCULATE QMIN THAT IS IN BOUNDS
                net_a.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                       # ADJUST QMIN IN POSITIVE DIRECTION WITH SOME EXTRA
                # print(shkey, 'Adj QMIN Up', 'QMIN =', qmin, 'NEW QMIN =', new_qmin)               # DEVELOPEMENT... PRINT MESSAGE
            elif qgen / voltage ** 2 > 0.98 * qmax > 0.0:                                           # CHECK IF QMAX IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmax = max(qmin, 0.99 * qmax * voltage ** 2)                                    # CALCULATE QMAX THAT IS IN BOUNDS
                net_a.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                       # ADJUST QMAX IN NEGATIVE DIRECTION WITH SOME EXTRA
                # print(shkey, 'Adj QMAX Down', 'QMAX =', qmax, 'NEW QMAX =', new_qmax)             # DEVELOPEMENT... PRINT MESSAGE
            pp.runpp(net_a, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW
    external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID REAL POWER
    external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                  # GET EXTERNAL GRID REACTIVE POWER

    # ---------------------------------------------------------------------------------------------
    # -- ZERO OUT EXTERNAL GRID REAL AND REACTIVE POWER (IF NEEDED)  ------------------------------
    # ---------------------------------------------------------------------------------------------
    external_pgen_threshold = 1e-4                                                                  # THRESHOLD FOR ZEROING OUT BASECASE EXTERNAL PGEN
    external_qgen_threshold = 1e-4                                                                  # THRESHOLD FOR ZEROING OUT BASECASE EXTERNAL QGEN
    zeroed = True                                                                                   # INITIALIZE ZEROED FLAG
    if abs(external_pgen) > external_pgen_threshold:                                                # IF EXTERNAL REAL POWER > THRESHOLD...
        zeroed = False                                                                              # SET ZEROED FLAG = FALSE
    if abs(external_qgen) > external_qgen_threshold:                                                # IF EXTERNAL REACTIVE POWER > THRESHOLD...
        zeroed = False                                                                              # SET ZEROED FLAG = FALSE
    zstep = 0                                                                                       # INITIALIZE ITERATOR
    if not zeroed:                                                                                  # IF EXTERNAL P AND Q IS NOT ZERO...
        p_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL P-UP MARGIN
        p_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL P-DOWN MARGIN
        p_upmargin_dict = {}                                                                        # INITIALIZE P-UP MARGIN DICT
        p_downmargin_dict = {}                                                                      # INITIALIZE P-DOWN MARGIN DICT
        for gkey in participating_gens:                                                             # LOOP THROUGH PARTICIPATING GENERATORS
            gidx = gen_keyidx[gkey]                                                                 # GET THIS PARTICIPATING GENERATOR INDEX
            pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                  # THIS GENERATORS PGEN
            pmin = net_a.gen.loc[gidx, 'min_p_mw']                                                  # THIS GENERATORS PMIN
            pmax = net_a.gen.loc[gidx, 'max_p_mw']                                                  # THIS GENERATORS PMAX
            p_upmargin = pmax - pgen                                                                # THIS GENERATORS P-UP MARGIN
            p_downmargin = pgen - pmin                                                              # THIS GENERATORS P-DOWN MARGIN
            p_upmargin_dict.update({gidx: p_upmargin})                                              # UPDATE P-UP MARGIN DICT
            p_upmargin_total += p_upmargin                                                          # INCREMENT TOTAL P-UP MARGIN
            p_downmargin_dict.update({gidx: p_downmargin})                                          # UPDATE P-DOWN MARGIN DICT
            p_downmargin_total += p_downmargin                                                      # INCREMENT TOTAL P-DOWN MARGIN
        q_participating_gens = []                                                                   # INITIALIZE QGEN INDEX LIST
        for gbus in genbuses:                                                                       # LOOP THROUGH GENERATOR BUSES
            if gbus == swingbus:                                                                    # IF GEN BUS IS SWINGBUS...
                continue                                                                            # GET NEXT GEN BUS
            if gbus in swshkey_dict:                                                                # IF GEN BUS HAS A SWITCHED SHUNT...
                continue                                                                            # GET NEXT GEN BUS
            gidxs = genidx_dict[gbus]                                                               # GET GENERATOR INDEXES ON THIS BUS
            if len(gidxs) == 1:                                                                     # CHECK IF ONLY ONE GENERATOR
                q_participating_gens += gidxs                                                       # ADD SINGLE GENERATOR ON BUS INDEX TO LIST
        q_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL Q-UP MARGIN
        q_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL Q-DOWN MARGIN
        q_upmargin_dict = {}                                                                        # INITIALIZE Q-UP MARGIN DICT
        q_downmargin_dict = {}                                                                      # INITIALIZE Q-DOWN MARGIN DICT
        for gidx in q_participating_gens:                                                           # LOOP THROUGH PARTICIPATING GENERATORS
            qgen = net_a.res_gen.loc[gidx, 'q_mvar']                                                # THIS GENERATORS QGEN
            qmin = net_a.gen.loc[gidx, 'min_q_mvar']                                                # THIS GENERATORS QMIN
            qmax = net_a.gen.loc[gidx, 'max_q_mvar']                                                # THIS GENERATORS QMAX
            q_upmargin = qmax - qgen                                                                # THIS GENERATORS Q-UP MARGIN
            q_downmargin = qgen - qmin                                                              # THIS GENERATORS Q-DOWN MARGIN
            q_upmargin_dict.update({gidx: q_upmargin})                                              # UPDATE Q-UP MARGIN DICT
            q_upmargin_total += q_upmargin                                                          # INCREMENT TOTAL Q-UP MARGIN
            q_downmargin_dict.update({gidx: q_downmargin})                                          # UPDATE Q-DOWN MARGIN DICT
            q_downmargin_total += q_downmargin                                                      # INCREMENT TOTAL Q-DOWN MARGIN
    while not zeroed and zstep < 20:                                                                # LIMIT WHILE LOOP ITERATIONS
        zeroed = True                                                                               # SET ZEROED FLAG = TRUE
        if abs(external_pgen) > external_pgen_threshold:                                            # CHECK IF EXTERNAL REAL POWER EXCEED THRESHOLD
            zeroed = False                                                                          # SET ZEROED FLAG
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_keyidx[gkey]                                                             # GET THIS PARTICIPATING GENERATOR INDEX
                pgen = net_a.res_gen.loc[gidx, 'p_mw']                                              # THIS GENERATORS REAL POWER
                if external_pgen < -external_pgen_threshold:                                        # CHECK IF EXTERNAL REAL POWER IS NEGATIVE
                    p_downmargin = p_downmargin_dict[gidx]                                          # GET THIS GENERATORS P-DOWN MARGIN
                    if p_downmargin < 1.0:                                                          # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_downmargin_dict[gidx] / p_downmargin_total       # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                if external_pgen > external_pgen_threshold:                                         # CHECK IF EXTERNAL REAL POWER IS POSITIVE
                    p_upmargin = p_upmargin_dict[gidx]                                              # GET THIS GENERATORS P-UP MARGIN
                    if p_upmargin < 1.0:                                                            # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_upmargin / p_upmargin_total                      # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK

        if abs(external_qgen) > external_qgen_threshold:                                            # CHECK IF EXTERNAL REACTIVE POWER EXCEED THRESHOLD
            zeroed = False                                                                          # SET ZEROED FLAG
            for gidx in q_participating_gens:                                                       # LOOP THROUGH PARTICIPATING GENERATORS
                vreg = net_a.res_gen.loc[gidx, 'vm_pu']                                             # THIS GENERATORS VOLTAGE SETPOINT
                if external_qgen < -external_qgen_threshold:                                        # CHECK IF EXTERNAL REACTIVE POWER IS NEGATIVE
                    q_downmargin = q_downmargin_dict[gidx]                                          # GET THIS GENERATORS Q-DOWN MARGIN
                    if vreg < 0.951 or q_downmargin < 1.0:                                          # IF NO MARGIN, OR BUS VOLTAGE IS LOW...
                        continue                                                                        # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total   # CALCULATE SETPOINT INCREMENT (PROPORTIONAL)
                    new_vreg = vreg + delta_vreg                                                        # CALCULATE NEW SET POINT
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                if external_qgen > external_qgen_threshold:                                         # CHECK IF EXTERNAL REACTIVE POWER IS POSITIVE
                    q_upmargin = q_upmargin_dict[gidx]                                              # GET THIS GENERATORS Q-UP MARGIN
                    if vreg > 1.049 or q_upmargin < 1.0:                                            # IF NO MARGIN, OR BUS VOLTAGE IS HIGH...
                        continue                                                                    # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_upmargin_dict[gidx] / q_upmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_vreg = vreg + delta_vreg                                                    # CALCULATE NEW SET POINT
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK

        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                # GET EXTERNAL GRID REAL POWER
        external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                              # GET EXTERNAL GRID REACTIVE POWER
        zstep += 1                                                                                  # INCREMENT ITERATOR

    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER
    base_cost = get_generation_cost(net_a, participating_gens, gen_keyidx, pwlcost_dict0)             # GET TOTAL COST OF GENERATION
    maxloading = get_maxloading(net_a, line_keyidx, xfmr_keyidx)
    minv, maxv = get_minmax_voltage(net_a)
    print('FINAL SCOPF NETWORK CREATED ........................................', round(time.time() - start_time, 3), 'sec')
    print('GENERATION COST ....................................................', '$ {0:.2f}'.format(base_cost))
    print('MAX BRANCH LOADING .................................................', maxloading)
    print('MIN BUS VOLTAGE ....................................................', '{0:.4f} pu'.format(minv))
    print('MAX BUS VOLTAGE ....................................................', '{0:.4f} pu'.format(maxv))

    # ---------------------------------------------------------------------------------------------
    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    # ---------------------------------------------------------------------------------------------
    print()
    print('WRITING BASECASE RESULTS TO FILE .... {0:.5f} MW {1:.5f} MVAR ......'.format(ex_pgen + 0.0, ex_qgen + 0.0))
    bus_results = copy.deepcopy(net_a.res_bus)                                                      # GET BASECASE BUS RESULTS
    gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
    write_base_bus_results(outfname, bus_results, swshidx_dict, gen_results, ext_grid_idx)          # WRITE SOLUTION1 BUS RESULTS
    write_base_gen_results(outfname, gen_results, Gids, genbuses, swshidxs)                         # WRITE SOLUTION1 GEN RESULTS

    print('DONE ---------------------------------------------------------------')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - master_start_time, 3))

    # == DEVELOPEMENT, COPY FILES FOR EVALUATION -------------------------------------------------- # TODO... Development copy results to directory
    if not sys.argv[1:]:
        import shutil
        dirname = os.path.dirname(__file__)
        shutil.copy(outfname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(raw_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(con_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(inl_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(rop_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(os.path.realpath(__file__), os.path.join(dirname, 'GitHub_Work/MyPython1.py'))
