import os
import sys
import csv
import math
import time
import numpy
from copy import deepcopy
import pandapower as pp
import data as data_read
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

    outfname = cwd + '//solution1.txt'
    NetworkModel = network + '  ' + scenario
    MaxRunningTime = 2700


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def read_data(raw_name, rop_name, inl_name, con_name):
    """read psse raw data"""
    p = data_read.Data()
    # print('READING RAW FILE ...................................................', os.path.split(raw_name)[1])
    if raw_name is not None:
        p.raw.read(os.path.normpath(raw_name))
    else:
        print('ERROR READING RAW FILE .............................................', os.path.split(raw_name)[1])
    # print('READING ROP FILE ...................................................', os.path.split(rop_name)[1])
    if rop_name is not None:
        p.rop.read(os.path.normpath(rop_name))
    else:
        print('ERROR READING ROP FILE .............................................', os.path.split(rop_name)[1])
    # print('READING INL FILE ...................................................', os.path.split(inl_name)[1])
    if inl_name is not None:
        p.inl.read(os.path.normpath(inl_name))
    else:
        print('ERROR READING INL FILE .............................................', os.path.split(inl_name)[1])
    # print('READING CON FILE....................................................', os.path.split(con_name)[1])
    if con_name is not None:
        p.con.read(os.path.normpath(con_name))
    else:
        print('ERROR READING CON FILE..............................................', os.path.split(con_name)[1])
    return p


def write_csvdata(fname, w_or_a, lol, label):
    """write csv data to file"""
    with open(fname, w_or_a, newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus):
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
    write_csvdata(fname, 'w', buslist, [['--bus section']])
    return


def write_gen_results(fname, g_results, genids, gbuses, swsh_idxs):
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
    write_csvdata(fname, 'a', glist, [['--generator section']])
    return


def get_swingbus_data(buses):
    """get swing bus data"""
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus = 0                                                                                       # initialze int
    swkv = 0.0                                                                                      # initialize float
    swangle = 0.0                                                                                   # initialize float
    for xbus in buses.values():                                                                     # loop through buses object
        if xbus.ide == 3:                                                                           # if bustype is swingbus...
            swbus = xbus.i                                                                          # get busnumber
            swkv = xbus.baskv                                                                       # get bus kv
            swangle = 0.0                                                                           # set swinbus angle = 0.0
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


def arghelper3(args):
    """ multiprocessing argument helper """
    return get_okey_pct(*args)


def parallel_get_okey_pct(arglist):
    """" prepare group data for parallel screening """
    numcpus = int(os.cpu_count())
    pool = multiprocessing.Pool(processes=numcpus)
    results = pool.map(arghelper3, arglist)
    pool.close()
    pool.join()
    return results


def get_okey_pct(xnet, okey, loadingthreshold, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp):
    """run powerflow on outage and check for overloads"""
    okeypct = []                                                                                    # initialize list
    net = deepcopy(xnet)                                                                            # get fresh copy of network
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
        print('No Solution Scanning Critical Outages', okey)
        return okeypct                                                                              # return empty list

    worst_line_loading = max(net.res_line['loading_percent'].values)
    worst_xfmr_loading = max(net.res_trafo['loading_percent'].values)
    worst_loading = max(worst_line_loading, worst_xfmr_loading)
    if worst_loading > loadingthreshold:
        okeypct = [worst_loading, okey]
    return okeypct


def run_outage_ac(xnet, okey, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, loadingthreshold, iteration):
    """run powerflow on outage and check for overloads"""
    mva_overloads = {okey: []}
    nosolveoutage = []
    net = deepcopy(xnet)                                                                            # get fresh copy of network
    if okey in onlinegens:                                                                          # check if outage is a generator...
        gidx = gendict[okey]                                                                        # get generator index
        pgen = net.res_gen.loc[gidx, 'p_mw']                                                        # get the outaged generator's pgen
        for loadkey in loaddict:                                                                    # loop through the loads (to minimize swing gen adjustment)
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
        print('NO SOLUTION RUNNING AC OUTAGE ....', okey)                                           # print statement
        nosolveoutage = [okey]                                                                      # assign outage key to nosolve outage list
        return {}, nosolveoutage                                                                    # return empty dict and nosolve outage key

    for lkey in linedict:                                                                           # loop across line keys
        lidx = linedict[lkey]                                                                       # get line index
        loading_pct = net.res_line.loc[lidx, 'loading_percent']                                     # get this line loading
        if loading_pct > loadingthreshold:                                                          # if loading greater than threshold...
            from_bus = xnet.line.loc[lidx, 'from_bus']                                              # get line frombus
            nom_kv = xnet.bus.loc[from_bus, 'vn_kv']                                                # get line nominal kv
            max_i_ka = xnet.line.loc[lidx, 'max_i_ka']                                              # get line maximum current
            mva_rating = nom_kv * max_i_ka * math.sqrt(3)                                           # calculate line mva rating
            mva = loading_pct * mva_rating / 100.0                                                  # calculate mva flow
            mva_overloading = mva - mva_rating                                                      # calculate mva overloading
            mva_overloads[okey].append(mva_overloading)                                             # add mva overloading to this outages list

    for xkey in xfmrdict:                                                                           # loop across xfmr keys
        xidx = xfmrdict[xkey]                                                                       # get xfmr index
        loading_pct = net.res_trafo.loc[xidx, 'loading_percent']                                    # get this xfmr loading
        if loading_pct > loadingthreshold:                                                          # if loading greater than threshold...
            mva_rating = xnet.trafo.loc[xidx, 'sn_mva']                                             # get mva rating
            mva = loading_pct * mva_rating / 100.0                                                  # get mva flow
            mva_overloading = mva - mva_rating                                                      # calculate mva overloading
            mva_overloads[okey].append(mva_overloading)                                             # add mva overloading to this outages list

    if not mva_overloads[okey]:                                                                     # if no overloads found...
        mva_overloads = {}                                                                          # assign empty dict to mva_overloads
    return mva_overloads, nosolveoutage


def arghelper1(args):
    """ multiprocessing argument helper """
    return run_outage_ac(*args)


def parallel_run_outage_ac(arglist):
    """" prepare group data for parallel screening """
    numcpus = int(os.cpu_count())
    pool = multiprocessing.Pool(processes=numcpus)
    results = pool.map(arghelper1, arglist)
    pool.close()
    pool.join()
    return results


def get_dominant_outages_ac(xnet, outagekeys, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp, loadingthreshold, basegencost, iteration):
    """get dominant outages resulting in branch loading, calls run_outages for multiprocessing"""

    mva_overloads_dict = {}                                                                         # declare dict
    nosolveoutages = []                                                                             # declare dict

    arglist = [[xnet, x, onlinegens, gendict, linedict, xfmrdict, loaddict, totalloadp,
                loadingthreshold, iteration] for x in outagekeys]                                   # create argument list for each process
    results = parallel_run_outage_ac(arglist)                                                       # get parallel outage results
    mva_overloads_i, nosolves_i = zip(*results)                                                     # transpose results and get overload and nosolve data
    for mva_overloads in mva_overloads_i:                                                           # loop across parallel overload results
        if not mva_overloads:                                                                       # if empty dict...
            continue                                                                                # get the next outage overloads
        mva_overloads_dict.update(mva_overloads)                                                    # add to master dict
    for nosolve in nosolves_i:                                                                      # loop across parallel nosolve results (likely empty list)
        nosolveoutages += nosolve                                                                   # add to master list

    # -- NEW METHOD WITH WEIGHTED OVERLOADS -------------------------------------------------------
    dominantoutages = []
    dominantoutagescost = []                                                                        # initialize list

    for okey in mva_overloads_dict:                                                                 # loop across outage keys
        outagecosts = numpy.interp(mva_overloads_dict[okey], [-1e6, 0.0, 0.01, 2.0, 2.01, 50.0, 50.01, 1e6], [0.0, 0.0, 1e3, 1e3, 5e3, 5e3, 1e6, 1e6])  # get list of costs of overloads
        totaloutagecost = sum(outagecosts)
        dominantoutagescost.append([totaloutagecost, okey])                                         # add the total cost and outage key to list
    dominantoutagescost.sort(reverse=True)                                                          # sort largest to smallest cost

    numoverloads = 0                                                                                # initialize number of overloads
    loadingcosts = []                                                                               # initialize list
    totalloadingcosts = []                                                                          # initialize list
    for data in dominantoutagescost:                                                                # loop through the overload cost outages list
        dominantoutages.append(data[1])                                                             # add outage key to list
        if data[0] > 1e-4:                                                                          # if overload for this outage...
            numoverloads += 1                                                                       # increment number of overloads
            loadingcosts.append(int(data[0]))                                                       # add cost for this outage to list
            totalloadingcosts.append(int(basegencost + data[0]))                                    # add generator cost + outage cost to list
        else:                                                                                       # if no overload for this outage...
            loadingcosts.append('-')                                                                # add placeholder for cost for this outage to list
            totalloadingcosts.append('-')                                                           # add placeholder for generator cost + outage cost to list

    print()
    print('{0:<2d}  DOMINANT OUTAGES'.format(iteration), dominantoutages[:10], '+ [{0:d}]'.format(max(0, len(dominantoutages) - 10)))
    print('{0:<2d}     BASE GEN COST'.format(iteration), round(basegencost, 1))
    # print('{0:<2d}    OVERLOAD COSTS'.format(iteration), loadingcosts[:10], '+ [{0:d}]'.format(max(0, len(loadingcosts) - 10)))
    print('{0:<2d}       TOTAL COSTS'.format(iteration), totalloadingcosts[:10], '+ [{0:d}]'.format(max(0, len(totalloadingcosts) - 10)))

    return dominantoutages, numoverloads, nosolveoutages


def arghelper2(args):
    """ multiprocessing argument helper """
    return run_outage_opf(*args)


def parallel_run_outage_opf(arglist):
    """" prepare outage data for parallel processing """
    numcpus = int(os.cpu_count())
    pool = multiprocessing.Pool(processes=numcpus)
    results = pool.map(arghelper2, arglist)
    pool.close()
    pool.join()
    return results


def run_outage_opf(xnet, okey, onlinegens, gendict, linedict, xfmrdict, genbusdict, swbus, swshdict, swshbusdict, swshkeys, extgrididx, iteration):
    """run opf on outage and return generator pgens"""
    opfgendict = {okey: {}}
    net = deepcopy(xnet)                                                                            # get fresh copy of this master network
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
    except:                                                                                                 # if no solution...
        print('{0:<2d} NO SOLUTION WITH OPF ....'.format(iteration), okey, '.... SKIP, GET NEXT OUTAGE')    # print nosolve message
        return opfgendict                                                                                   # get next contingency
    net = copy_opf_to_network(net, net, gendict, genbusdict, swbus, swshdict, swshbusdict, extgrididx)      # copy opf results to this network
    pp.runpp(net, enforce_q_lims=True)                                                                      # solve this network with powerflow
    swshgkeys = swshkeys + onlinegens                                                                       # combine swshkeys and generator keys
    opfgendict[okey] = get_swsh_gen_data(net, swshgkeys, gendict, swshdict)                                 # get generator data for this network
    return opfgendict


def get_swsh_gen_data(xnet, swshgkeys, gendict, swshdict):
    """get generators pgen for this network"""
    basegendata = {}                                                                                # initialize dict
    for swshgkey in swshgkeys:                                                                      # loop across generator and swshunt keys
        if swshgkey in gendict:                                                                     # if a generator...
            g_idx = gendict[swshgkey]                                                               # get generator index
            pgen = xnet.res_gen.loc[g_idx, 'p_mw']                                                  # get generator pgen
            gvreg = xnet.res_gen.loc[g_idx, 'vm_pu']                                                # get generator vreg
            qgen = xnet.res_gen.loc[g_idx, 'q_mvar']                                                # get generator qgen
        if swshgkey in swshdict:                                                                    # if a swshunt...
            swsh_idx = swshdict[swshgkey]                                                           # get swshunt index
            pgen = 0.0                                                                              # swshunt pgen=0.0
            gvreg = xnet.res_gen.loc[swsh_idx, 'vm_pu']                                             # get swshunt vreg
            qgen = xnet.res_gen.loc[swsh_idx, 'q_mvar']                                             # get swshunt qgen
        basegendata.update({swshgkey: [pgen, gvreg, qgen]})                                         # update dict with {swsh_gkey:[pgen,gvreg,qgen]}
    return basegendata


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
    max_loading = 0.0                                                                               # initialize max branch loading
    bkey = ''                                                                                       # initialize branch key
    for lkey in linedict:                                                                           # loop through the lines
        lidx = linedict[lkey]                                                                       # get line index
        loading = xnet.res_line.loc[lidx, 'loading_percent']                                        # get the line loading
        if loading > max_loading:                                                                   # if loading > max loading..
            max_loading = loading                                                                   # assign new max loading
            bkey = lkey                                                                             # assign branch key
    for xkey in xfmrdict:                                                                           # loop through the xfmrs
        xidx = xfmrdict[xkey]                                                                       # get xfmr index
        loading = xnet.res_trafo.loc[xidx, 'loading_percent']                                       # get the xfmr loading
        if loading > max_loading:                                                                   # if loading > max loading..
            max_loading = loading                                                                   # assign new max loading
            bkey = xkey                                                                             # assign branch key
    max_loading = round(max_loading, 3)                                                             # round maxloading to 3 places
    return [max_loading, bkey]


def get_minmax_voltage(xnet):
    """get max and min bus voltage for this network"""
    min_voltage = min(xnet.res_bus['vm_pu'].values)                                                 # get max bus voltage
    max_voltage = max(xnet.res_bus['vm_pu'].values)                                                 # get min bus voltage
    return min_voltage, max_voltage


def finetune_network(xnet, onlinegens, gendict, genidxdict, swshidxdict, extgrididx):
    """fine tune network, zero out external grid, check voltage schedules, check swshunt susceptance"""

    # -- MAKE SHURE GENERATORS WITH PMAX=0 HAVE PGEN=0 --------------------------------------------
    for gkey in gendict:                                                                           # loop across generator keys
        gidx = gendict[gkey]                                                                       # get generator index
        if xnet.gen.loc[gidx, 'max_p_mw'] == 0.0:                                                  # CHECK IF PMAX=0
            xnet.gen.loc[gidx, 'p_mw'] = 0.0                                                       # SET PGEN=0

    # -- INSURE GENERATORS ARE MEETING VOLTAGE SCHEDULE -------------------------------------------
    for genbus in genidxdict:
        gen_off_schedule = False                                                                    # RESET GENERATOR OFF SCHEDULE FLAG
        bus_voltage = None                                                                          # INITIALIZE BUS VOLTAGE
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        gen_indexes = genidxdict[genbus]                                                            # GET GENERATOR INDEX
        for gidx in gen_indexes:                                                                    # LOOP ACROSS GENERATORS ON THIS BUS
            qgen = xnet.res_gen.loc[gidx, 'q_mvar']                                                 # THIS GENERATORS QGEN
            qmin = xnet.gen.loc[gidx, 'min_q_mvar']                                                 # THIS GENERATORS QMIN
            qmax = xnet.gen.loc[gidx, 'max_q_mvar']                                                 # THIS GENERATORS QMAX
            gvreg = xnet.res_gen.loc[gidx, 'vm_pu']
            bus_voltage = xnet.res_bus.loc[genbus, 'vm_pu']                                         # get this generators bus voltage
            if qgen == qmin or qgen == qmax or gvreg != bus_voltage:                                # if this generator at +/- qlimit or no meeting vreg
                gen_off_schedule = True                                                             # set off schedule flag = true
                break                                                                               # break and loop across this buses generators
        if gen_off_schedule:                                                                        # generator found off schedule...
            for gidx in gen_indexes:                                                                # loop across generators on this bus
                xnet.gen.loc[gidx, 'vm_pu'] = bus_voltage                                           # this network, set this generators vreg to bus voltage
            if genbus in swshidxdict:                                                               # check if there is a swshunt on this gen bus
                shidx = swshidxdict[genbus]                                                         # get swshunt index
                xnet.gen.loc[shidx, 'vm_pu'] = bus_voltage                                          # also set swshunt vreg to bus voltage
            pp.runpp(xnet, init='results', enforce_q_lims=True)                                     # this network, run straight power flow

    # -- INSURE SWSHUNTS SUSCEPTANCE IS WITHIN LIMITS IN BASECASE ---------------------------------
    # -- TODO IS THIS NEEDED NOW WITH CARLETON'S SOLUTION 2 CODE?
    # -- HOPE CONSERVATIVE ENOUGH TO HOLD UP WITH CONTINGENCIES -----------------------------------
    for shkey in swsh_keyidx:                                                                       # loop across swshunt keys
        shidx = swsh_keyidx[shkey]                                                                  # get swshunt index
        shbus = swshbus_dict[shidx]                                                                 # get swshunt bus
        qgen = xnet.res_gen.loc[shidx, 'q_mvar']                                                    # get swshunt qgen
        qmin = xnet.gen.loc[shidx, 'min_q_mvar']                                                    # get minimum swshunt reactive capability
        qmax = xnet.gen.loc[shidx, 'max_q_mvar']                                                    # get maximum swshunt reactive capability
        voltage = xnet.res_bus.loc[shbus, 'vm_pu']                                                  # get swshunt bus voltage
        if voltage < 1.0:                                                                           # if bus voltage is < 1.0 (susceptance could be exceeded)
            if qgen / voltage ** 2 < 0.98 * qmin < 0.0:                                             # check if qmin is negative and susceptance out of bounds
                new_qmin = min(qmax, 0.99 * qmin * voltage ** 2)                                    # calculate qmin that is in bounds
                xnet.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                        # adjust qmin in positive direction with some extra
            elif qgen / voltage ** 2 > 0.98 * qmax > 0.0:                                           # check if qmax is negative and susceptance out of bounds
                new_qmax = max(qmin, 0.99 * qmax * voltage ** 2)                                    # calculate qmax that is in bounds
                xnet.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                        # adjust qmax in negative direction with some extra
            pp.runpp(xnet, init='results', enforce_q_lims=True)                                     # this network, run straight power flow
    externalpgen = xnet.res_ext_grid.loc[extgrididx, 'p_mw']                                        # get external grid real power
    externalqgen = xnet.res_ext_grid.loc[extgrididx, 'q_mvar']                                      # get external grid reactive power

    # ---------------------------------------------------------------------------------------------
    # -- ZERO OUT EXTERNAL GRID REAL AND REACTIVE POWER (IF NEEDED)  ------------------------------
    # ---------------------------------------------------------------------------------------------
    external_pgen_threshold = 1e-4                                                                  # threshold for zeroing out basecase external pgen
    external_qgen_threshold = 1e-4                                                                  # threshold for zeroing out basecase external qgen
    zeroed = True                                                                                   # initialize zeroed flag
    if abs(externalpgen) > external_pgen_threshold:                                                 # if external real power > threshold...
        zeroed = False                                                                              # set zeroed flag = false
    if abs(externalqgen) > external_qgen_threshold:                                                 # if external reactive power > threshold...
        zeroed = False                                                                              # set zeroed flag = false
    zstep = 0                                                                                       # initialize iterator
    if not zeroed:                                                                                  # if external p and q is not zero...
        p_upmargin_total = 0.0                                                                      # initialize total p-up margin
        p_downmargin_total = 0.0                                                                    # initialize total p-down margin
        p_upmargin_dict = {}                                                                        # initialize p-up margin dict
        p_downmargin_dict = {}                                                                      # initialize p-down margin dict
        for gkey in onlinegens:                                                                     # loop through online generators
            gidx = gendict[gkey]                                                                    # get this participating generator index
            pgen = xnet.res_gen.loc[gidx, 'p_mw']                                                   # this generators pgen
            pmin = xnet.gen.loc[gidx, 'min_p_mw']                                                   # this generators pmin
            pmax = xnet.gen.loc[gidx, 'max_p_mw']                                                   # this generators pmax
            p_upmargin = pmax - pgen                                                                # this generators p-up margin
            p_downmargin = pgen - pmin                                                              # this generators p-down margin
            p_upmargin_dict.update({gidx: p_upmargin})                                              # update p-up margin dict
            p_upmargin_total += p_upmargin                                                          # increment total p-up margin
            p_downmargin_dict.update({gidx: p_downmargin})                                          # update p-down margin dict
            p_downmargin_total += p_downmargin                                                      # increment total p-down margin
        q_participating_gens = []                                                                   # initialize qgen index list
        for gbus in genbuses:                                                                       # loop through generator buses
            if gbus == swingbus:                                                                    # if gen bus is swingbus...
                continue                                                                            # get next gen bus
            if gbus in swshkey_dict:                                                                # if gen bus has a switched shunt...
                continue                                                                            # get next gen bus
            gidxs = genidxdict[gbus]                                                                # get generator indexes on this bus
            if len(gidxs) == 1:                                                                     # check if only one generator
                q_participating_gens += gidxs                                                       # add single generator on bus index to list
        q_upmargin_total = 0.0                                                                      # initialize total q-up margin
        q_downmargin_total = 0.0                                                                    # initialize total q-down margin
        q_upmargin_dict = {}                                                                        # initialize q-up margin dict
        q_downmargin_dict = {}                                                                      # initialize q-down margin dict
        for gidx in q_participating_gens:                                                           # loop through participating generators
            qgen = xnet.res_gen.loc[gidx, 'q_mvar']                                                 # this generators qgen
            qmin = xnet.gen.loc[gidx, 'min_q_mvar']                                                 # this generators qmin
            qmax = xnet.gen.loc[gidx, 'max_q_mvar']                                                 # this generators qmax
            q_upmargin = qmax - qgen                                                                # this generators q-up margin
            q_downmargin = qgen - qmin                                                              # this generators q-down margin
            q_upmargin_dict.update({gidx: q_upmargin})                                              # update q-up margin dict
            q_upmargin_total += q_upmargin                                                          # increment total q-up margin
            q_downmargin_dict.update({gidx: q_downmargin})                                          # update q-down margin dict
            q_downmargin_total += q_downmargin                                                      # increment total q-down margin

    while not zeroed and zstep < 20:                                                                # limit while loop iterations
        zeroed = True                                                                               # set zeroed flag = true
        if abs(externalpgen) > external_pgen_threshold:                                             # check if external real power exceed threshold
            zeroed = False                                                                          # set zeroed flag
            for gkey in onlinegens:                                                                 # loop through online generators
                gidx = gendict[gkey]                                                                # get this participating generator index
                pgen = xnet.res_gen.loc[gidx, 'p_mw']                                               # this generators real power
                if externalpgen < -external_pgen_threshold:                                         # check if external real power is negative
                    p_downmargin = p_downmargin_dict[gidx]                                          # get this generators p-down margin
                    if p_downmargin < 1.0:                                                          # if no margin...
                        continue                                                                    # get next generator
                    delta_pgen = externalpgen * p_downmargin_dict[gidx] / p_downmargin_total        # calculate generator increment (distributed proportionalLY)
                    new_pgen = pgen + delta_pgen                                                    # calculate  generator next pgen
                    xnet.gen.loc[gidx, 'p_mw'] = new_pgen                                           # set generator pgen for this network
                if externalpgen > external_pgen_threshold:                                          # check if external real power is positive
                    p_upmargin = p_upmargin_dict[gidx]                                              # get this generators p-up margin
                    if p_upmargin < 1.0:                                                            # if no margin...
                        continue                                                                    # get next generator
                    delta_pgen = externalpgen * p_upmargin / p_upmargin_total                       # calculate generator increment (distributed proportionalLY)
                    new_pgen = pgen + delta_pgen                                                    # calculate  generator next pgen
                    xnet.gen.loc[gidx, 'p_mw'] = new_pgen                                           # set generator pgen for this network

        if abs(externalqgen) > external_qgen_threshold:                                             # check if external reactive power exceed threshold
            zeroed = False                                                                          # set zeroed flag
            for gidx in q_participating_gens:                                                       # loop through participating generators
                vreg = xnet.res_gen.loc[gidx, 'vm_pu']                                              # this generators voltage setpoint
                if externalqgen < -external_qgen_threshold:                                         # check if external reactive power is negative
                    q_downmargin = q_downmargin_dict[gidx]                                          # get this generators q-down margin
                    if vreg < 0.951 or q_downmargin < 1.0:                                          # if no margin, or bus voltage is low...
                        continue                                                                        # if so, get next generator
                    delta_vreg = 0.020 * externalqgen * q_downmargin_dict[gidx] / q_downmargin_total    # calculate setpoint increment (proportional)
                    new_vreg = vreg + delta_vreg                                                        # calculate new set point
                    xnet.gen.loc[gidx, 'vm_pu'] = new_vreg                                          # set generator qgen for this network
                if externalqgen > external_qgen_threshold:                                          # check if external reactive power is positive
                    q_upmargin = q_upmargin_dict[gidx]                                              # get this generators q-up margin
                    if vreg > 1.049 or q_upmargin < 1.0:                                            # if no margin, or bus voltage is high...
                        continue                                                                    # if so, get next generator
                    delta_vreg = 0.020 * externalqgen * q_upmargin_dict[gidx] / q_upmargin_total    # calculate setpoint increment (distributed proportionally)
                    new_vreg = vreg + delta_vreg                                                    # calculate new set point
                    xnet.gen.loc[gidx, 'vm_pu'] = new_vreg                                          # set generator qgen for this network

        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        externalpgen = net_a.res_ext_grid.loc[extgrididx, 'p_mw']                                   # GET EXTERNAL GRID REAL POWER
        externalqgen = net_a.res_ext_grid.loc[extgrididx, 'q_mvar']                                 # GET EXTERNAL GRID REACTIVE POWER
        zstep += 1                                                                                  # INCREMENT ITERATOR
    return xnet


# =================================================================================================
# -- MYPYTHON_1 --- GO CHALLENGE 2019 -------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    master_start_time = time.time()                                                                 # INITIALIZE MAIN PROGRAM START TIME
    print()                                                                                         # PRINT STATEMENT
    print('===================  ' + NetworkModel + '  ===================')
    print('MAX RUNNING TIME =', MaxRunningTime)
    MaxBaseLoading = 95.0                                                                           # MAXIMUM %BRANCH LOADING FOR N-0 AND N-1

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
    # if UseNetC:
    #     net_c = pp.create_empty_network('net_c', 60.0, mva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.name, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    NumBuses = 0
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

        if busnum == swingbus:
            swingbus_idx = idx
        NumBuses += 1
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
        # -- IF REAL LOAD -----------------------------------------------------
        if loadp >= 0.0:
            idx = pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadkey, controllable=False)
            if status:
                load_keyidx.update({loadkey: idx})
                total_loadp += loadp
        # -- IF NEG LOAD ------------------------------------------------------
        if loadp < 0.0:
            idx = pp.create_sgen(net_a, loadbus, p_mw=-loadp, q_mvar=-loadq, sn_mva=loadmva, name=loadkey)

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
                                vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            participating_gens.append(genkey)
            area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)

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
                                vn_kv=nomkv, type='GEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            participating_gens.append(genkey)
            area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                vn_kv=nomkv,  type='GEN', controllable=True, in_service=status)

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
            # if UseNetC:
            #     pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxshidx_dict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
    swshkeys = []

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
                                vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH')
            swshidx_dict.update({shuntbus: idx})
            swshidxs.append(idx)
            swshkeys.append(swshkey)
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
        idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey, max_loading_percent=MaxBaseLoading, in_service=status)

        line_keyidx.update({linekey: idx})
        lineidxs.append(idx)
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
                                                    in_service=status, name=xfmrkey, max_loading_percent=MaxBaseLoading)
        xfmr_keyidx.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmr_ratec_dict.update({xfmrkey: xfmr.ratc1})
        xfmridxs.append(idx)
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

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 2))
    # =============================================================================================
    # -- NETWORKS CREATED -------------------------------------------------------------------------
    # =============================================================================================

    # ---------------------------------------------------------------------------------------------
    # -- SOLVE NETWORK WITH POWERFLOW AND OPF -----------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    solve_starttime = time.time()                                                                   # INITIALIZE START-TIME
    net = deepcopy(net_a)                                                                           # GET COPY OF BASECASE NETWORK
    try:                                                                                            # TRY RUNNING POWERFLOW THE OPF
        solve_starttime = time.time()                                                               # INITIALIZE START-TIME
        pp.runpp(net, enforce_q_lims=True)                                                          # RUN POWERFLOW ON THIS NETWORK
        print('   POWERFLOW SOLVED ................................................', round(time.time() - solve_starttime, 2))
        solve_starttime = time.time()                                                               # INITIALIZE START-TIME
        pp.runopp(net, init='pf')                                                                   # RUN OPF ON THIS NETWORK
        # pp.runopp(net, init='pf', OPF_VIOLATION=1e-4, PDIPM_COSTTOL=1e-4, PDIPM_GRADTOL=1e-4)     # RUN OPF ON THIS NETWORK
        opf_time = time.time() - solve_starttime                                               # CALCULATE OPF SOLVE TIME
        print('   OPTIMAL POWERFLOW SOLVED ........................................', round(opf_time, 2))
        net_a = copy_opf_to_network(net, net_a, gen_keyidx, genbus_dict, swingbus, swsh_keyidx,
                                    swshbus_dict, ext_grid_idx)                                     # COPY OPF RESULTS TO THIS NETWORK
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN POWERFLOW ON NETA

    except RuntimeError:                                                                            # IF FIRST POWERFLOW DID NOT SOLVE
        net = deepcopy(net_a)                                                                       # GET COPY OF BASECASE NETWORK
        solve_starttime = time.time()                                                               # INITIALIZE START-TIME
        pp.runopp(net, init='pf')                                                                   # RUN OPF ON THIS NETWORK
        # pp.runopp(net, init='pf', OPF_VIOLATION=1e-4, PDIPM_COSTTOL=1e-4, PDIPM_GRADTOL=1e-4)     # RUN OPF ON THIS NETWORK
        opf_time = time.time() - solve_starttime                                               # CALCULATE OPF SOLVE TIME
        print('   OPTIMAL POWERFLOW SOLVED FIRST ..................................', round(opf_time, 3), 'sec')
        net_a = copy_opf_to_network(net, net_a, gen_keyidx, genbus_dict, swingbus, swsh_keyidx,
                                    swshbus_dict, ext_grid_idx)                                     # COPY OPF RESULTS TO THIS NETWORK
        solve_starttime = time.time()                                                               # INITIALIZE START-TIME
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN POWERFLOW ON NETA
        print('   POWERFLOW SOLVED ................................................', round(time.time() - solve_starttime, 3), 'sec')

    net_a = finetune_network(net_a, online_gens, gen_keyidx, genidx_dict, swshidx_dict, ext_grid_idx)
    # -----------------------------------------------------------------------------------------
    # -- WRITE BUS AND GENERATOR RESULTS TO FILE (INCASE OF CRASH OR TIME LIMIT) --------------
    # -----------------------------------------------------------------------------------------
    bus_results = deepcopy(net_a.res_bus)                                                           # GET BASECASE BUS RESULTS
    gen_results = deepcopy(net_a.res_gen)                                                           # GET BASECASE GENERATOR RESULTS
    write_bus_results(outfname, bus_results, swshidx_dict, gen_results, ext_grid_idx)               # WRITE SOLUTION1 BUS RESULTS
    write_gen_results(outfname, gen_results, Gids, genbuses, swshidxs)                              # WRITE SOLUTION1 GEN RESULTS
    # -----------------------------------------------------------------------------------------
    last_known_good_net = deepcopy(net_a)

    # =============================================================================================
    # -- TRY TO FILTER OUT SOME GENERATOR AND BRANCH OUTAGES  -------------------------------------
    # =============================================================================================
    goutage_keys = list(outage_dict['gen'].keys())                                                  # GET OUTAGED GENERATOR KEYS
    boutage_keys = list(outage_dict['branch'].keys())                                               # GET OUTAGED BRANCH KEYS
    goutage_keys = [x for x in goutage_keys if x not in zero_gens]                                  # REMOVE ANY GENERATOR OUTAGES WITH PGEN=0.0 or OFF-LINE
    boutage_keys = [x for x in boutage_keys if x not in zero_branches]                              # REMOVE ANY BRANCH OUTAGES if OPEN
    outage_keys = goutage_keys + boutage_keys                                                       # SET HOW MANY BRANCH OUTAGES TO CONSIDER
    NumOutages_0 = len(outage_keys)

    Screen_Outages = False
    if Screen_Outages:
        # -- SCREEN FOR OUTAGES RESULTING IN LOADING > MAXBASELOADING -------------
        oscreen_starttime = time.time()
        loading_threshold = MaxBaseLoading + 1.0
        # -- GET GENERATOR OUTAGES RESULTING IN LOADING OVER THRESHOLD ------------
        arglist = [[net_a, x, loading_threshold, online_gens, gen_keyidx, line_keyidx, xfmr_keyidx, load_keyidx, total_loadp] for x in goutage_keys]
        pct_gkeys = parallel_get_okey_pct(arglist)
        pct_gkeys = [x for x in pct_gkeys if x]
        pct_gkeys.sort(reverse=True)
        goutage_keys = [x[1] for x in pct_gkeys]

        # -- GET BRANCH OUTAGES RESULTING IN LOADING OVER THRESHOLD ---------------
        arglist = [[net_a, x, loading_threshold, online_gens, gen_keyidx, line_keyidx, xfmr_keyidx, load_keyidx, total_loadp] for x in boutage_keys]
        pct_bkeys = parallel_get_okey_pct(arglist)
        pct_bkeys = [x for x in pct_bkeys if x]
        pct_bkeys.sort(reverse=True)
        boutage_keys = [x[1] for x in pct_bkeys]

        # -- COMBINE GEN AND BRANCH OUTAGES RESULTING IN LOADING OVER THRESHOLD ---
        pct_okeys = pct_gkeys + pct_bkeys
        pct_okeys.sort(reverse=True)
        outage_keys = [x[1] for x in pct_okeys]
        print('CRTICAL OUTAGES FOUND ..............................................', round(time.time() - oscreen_starttime, 2))

    NumOutages_1 = len(outage_keys)
    print('BUSES ..............................................................', NumBuses)
    print('OUTAGES ............................................................', NumOutages_0, '-', NumOutages_1)

    # *********************************************************************************************
    # -- FIND BASECASE OPF OPERATING POINT --------------------------------------------------------
    # *********************************************************************************************
    print('-------------------- ATTEMPTING BASECASE SCOPF ---------------------')
    c_net = deepcopy(net_a)                                                                         # GET COPY OF THE RATEA NETWORK
    processed_outages = []                                                                          # INITIALIZE LIST OF ALREADY PROCESSED OUTAGES
    nosolve_outages = []

    opf_gendata_dict = {}                                                                           # PARALLEL OPF GENERATOR DICT
    pgen_minmax_dict = {}                                                                           # PARALLEL GENERATOR MIN-MAX P CHANGE DICT
    vreg_minmax_dict = {}                                                                           # PARALLEL GENERATOR MIN-MAX VREG CHANGE DICT
    qgen_minmax_dict = {}                                                                           # PARALLEL GENERATOR MIN-MAX Q CHANGE DICT

    gpdelta_threshold = 1.2                                                                         # ESTIMATE HOW MUCH PGEN CHANGE IS SIGNIFICANT
    gvregdelta_threshold = 0.020                                                                    # ESTIMATE HOW MUCH GEN OR SWSHUNT VREG CHANGE IS SIGNIFICANT
    num_parallel_outages = 1                                                                        # ASSIGN THE NUMBER OF PARALLEL OUTAGES

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    # -- LOOP WHILE THERE ARE REMAINING DOMINANT OUTAGES ------------------------------------------
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    scopf_start_time = time.time()                                                                  # SET THE WHILE LOOP START TIME
    start_iteration_time = time.time() - opf_time                                                   # INITIALIZE FIRST START ITERATION TIME
    last_iteration_time = 0.0                                                                       # INITIALIZE TIME FOR EACH WHILE LOOP ITERATION
    max_iteration_time = 0.0                                                                        # INITIALIZE MAXIMUM ITERATION TIME
    finalize_time = opf_time + 60.0                                                                 # ESTIMATE THE TIME TO FINALIZE SCOPF BASECASE
    elapsed_time = round(time.time() - master_start_time, 3)                                        # GET THE ELAPSED TIME SO FAR

    countdown_time = MaxRunningTime - elapsed_time - finalize_time                                  # INITIALIZE COUNTDOWN TIME
    dominant_outages = [x for x in outage_keys]                                                     # INITIALIZE DOMINANT OUTAGES
    nosolves_reprocessed = False                                                                    # INITIALIZE REPROCESS NOSOLVES FLAG
    step = 0                                                                                        # INITIALIZE ITERATOR

    while countdown_time > 0.0:                                                                     # LOOP WHILE TIME REMAINS
        pp.runpp(c_net, enforce_q_lims=True)                                                        # SOLVE THIS MASTER BASECASE
        if countdown_time < max_iteration_time:                                                     # CHECK IF NOT ENOUGH TIME TO FOR NEXT FULL RUN...
            break                                                                                   # EXIT AND FINE-TUNE RESULTS SO FAR
        base_gen_data = get_swsh_gen_data(c_net, online_gens + swshkeys, gen_keyidx, swsh_keyidx)   # GET GENERATORS PGEN AND VREG FOR THIS MASTER BASECASE
        base_gen_cost = get_generation_cost(c_net, participating_gens, gen_keyidx, pwlcost_dict0)   # GET TOTAL COST OF THIS BASECASE GENERATION

        # -- GET DOMINANT OUTAGES RESULTING IN BRANCH OVERLOADING ---------------------------------
        overloaded_outages = []                                                                     # ASSUME NO BRANCHES ARE OVERLOADED
        dominant_outages = [x for x in dominant_outages if x not in processed_outages]              # UPDATE DOMINANT OUTAGE KEYS TO BE PROCESSED

        if dominant_outages:                                                                        # IF THERE ARE DOMINANT OUTAGES TO BE PROCESSES...
            dominant_outages, num_overloads, nosolves \
                = get_dominant_outages_ac(c_net, dominant_outages, online_gens, gen_keyidx,
                                          line_keyidx, xfmr_keyidx, load_keyidx, total_loadp,
                                          max(100.0, 95.0 + step), base_gen_cost, step)             # GET DOMINANT GENERATOR AND BRANCH OUTAGES
            nosolve_outages += nosolves                                                             # ADD ANY NOSOLVE OUTAGE KEYS FOUND
            overloaded_outages = dominant_outages[:num_overloads]                                   # GET THE OUTAGES RESULTING IN OVERLOADS

        if not overloaded_outages:                                                                  # CHECK IF NO MORE OVERLOADS...
            if nosolve_outages and not nosolves_reprocessed:                                        # IF THERE ARE NOSOLVES AND HAVE NOT ATTEMPTED TO PROCESS...
                nosolves_reprocessed = True                                                         # SET THE SECOND CYCLE FLAG=TRUE
                if countdown_time > max_iteration_time + 30.0:                                      # IF TIME REMAINING TO START PROCESSING NOSOLVES...
                    print()                                                                         # PRINT BLANK LINE
                    print('RUNNING NOSOLVE OUTAGES', nosolve_outages, len(nosolve_outages))         # PRINT MESSAGE
                    nosolve_outages = list(set(nosolve_outages))
                    dominant_outages = [x for x in nosolve_outages]                                 # ASSIGN NOSOLVES TO OUTAGES TO RUN
                    nosolve_outages = []                                                            # CLEAR THE NOSOLVE OUTAGES
                    processed_outages = []                                                          # CLEAR THE PROCESSED OUTAGES
                    step += 1                                                                       # INCREMENT ITERATOR
                    continue                                                                        # START PROCESSING THE NOSOLVE OUTAGES
            else:
                break                                                                               # EXIT AND FINE-TUNE RESULTS SO FAR

        o_keys = overloaded_outages[:num_parallel_outages]                                          # SET HOW MANY OVERLOADED OUTAGES TO PROCESS
        last_iteration_time = time.time() - start_iteration_time                                    # CALCULATE THIS ITERATION TIME
        max_iteration_time = max(last_iteration_time, max_iteration_time)                           # CHECK FOR MAX ITERATION TIME

        print('{0:<2d} PROCESSED OUTAGES'.format(step), processed_outages, len(processed_outages))  # PRINT STATEMENT
        print('{0:<2d}   NOSOLVE OUTAGES'.format(step), nosolve_outages, len(nosolve_outages))      # PRINT STATEMENT
        print('{0:<2d}    NOW PROCESSING'.format(step), o_keys)                                     # PRINT STATEMENT
        print('{0:<2d}    ITERATION TIME'.format(step), round(last_iteration_time, 1))              # PRINT STATEMENT
        print('{0:<2d}    COUNTDOWN TIME'.format(step), round(countdown_time, 1))                   # PRINT STATEMENT

        # ==========================================================================================
        # == RUN THE OVERLOADED OUTAGES ON THIS BASECASE WITH OPF ==================================
        # ==========================================================================================
        start_iteration_time = time.time()                                                          # INITIALIZE START ITERATION TIME
        # :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        if len(o_keys) == 1:                                                                        # IF ONLY ONE OUTAGE IN GROUP... PROCESS IT
            o_key = o_keys[0]                                                                       # GET THE OUTAGE KEY
            net = deepcopy(c_net)                                                                   # GET FRESH COPY OF THIS MASTER NETWORK
            if o_key in gen_keyidx:                                                                 # CHECK IF A GENERATOR...
                g_idx = gen_keyidx[o_key]                                                           # GET GENERATOR INDEX
                net.gen.in_service[g_idx] = False                                                   # SWITCH OFF OUTAGED GENERATOR
            elif o_key in line_keyidx:                                                              # CHECK IF A LINE...
                line_idx = line_keyidx[o_key]                                                       # GET LINE INDEX
                net.line.in_service[line_idx] = False                                               # SWITCH OUT OUTAGED LINE
            elif o_key in xfmr_keyidx:                                                              # CHECK IF A XFMR...
                xfmr_idx = xfmr_keyidx[o_key]                                                       # GET XFMR INDEX
                net.trafo.in_service[xfmr_idx] = False                                              # SWITCH OUT OUTAGED XFMR
            pp.runpp(net, enforce_q_lims=True)                                                      # SOLVE THIS NETWORK WITH POWERFLOW
            try:                                                                                    # TRY OPF
                pp.runopp(net, init='pf')                                                           # RUN OPF ON THIS NETWORK
            except:                                                                                 # IF NO SOLUTION...
                print('{0:<2d}  NOSOLVE WITH OPF'.format(step), o_keys, 'REMOVE FROM OUTAGES')      # PRINT NOSOLVE MESSAGE
                processed_outages.append(o_key)                                                     # ADD THIS OUTAGE TO PROCESSED OUTAGES
                nosolve_outages.append(o_key)                                                       # ADD OUTAGE KEY TO NOSOLVE OUTAGES
                elapsed_time = time.time() - master_start_time                                      # GET THE ELAPSED TIME SO FAR
                countdown_time = MaxRunningTime - elapsed_time - opf_time - finalize_time           # CALCULATE TIME LEFT
                step += 1                                                                           # INCREMENT ITERATOR
                c_net = deepcopy(last_known_good_net)                                               # COPY LAST KNOWN GOOD NET TO CNET
                continue                                                                            # ELSE... GET NEXT DOMINANT OUTAGES

            # -- SINCE THIS 'NET' OUTAGE SOLVED WITH OPF ------------------------------------------
            last_known_good_net = deepcopy(c_net)                                                   # SAVE LAST ITERATION CNET AS LAST KNOW GOOD NET

            c_net = copy_opf_to_network(net, c_net, gen_keyidx, genbus_dict, swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx)  # COPY OPF RESULTS TO NEXT CNET

            if o_key in gen_keyidx:                                                                 # CHECK IF OUTAGE WAS A GENERATOR...
                c_net.gen.in_service[g_idx] = True                                                  # SWITCH ON THE OUTAGED GENERATOR
                c_net.gen.loc[g_idx, 'p_mw'] = base_gen_data[o_key][0]                              # RESET GENERATOR PGEN
            elif o_key in line_keyidx:                                                              # CHECK IF OUTAGE WAS A LINE...
                c_net.line.in_service[line_idx] = True                                              # SWITCH ON THE OUTAGED LINE
            elif o_key in xfmr_keyidx:                                                              # CHECK OUTAGE WAS A XFMR...
                c_net.trafo.in_service[xfmr_idx] = True                                             # SWITCH ON THE OUTAGED XFMR
            pp.runpp(c_net, enforce_q_lims=True)                                                    # SOLVE THIS NETWORK WITH POWERFLOW

            # -- GET GENERATORS PGEN FROM OPF RESULTS (SET PMAX OR PMIN IF SIGNIFICANT CHANGE) ----
            swsh_gkeys = swshkeys + online_gens
            for g_key in swsh_gkeys:                                                                # LOOP ACROSS GENERATORS AND SWSHUNTS
                if g_key == o_key:                                                                  # CHECK IF THIS OUTAGE IS THIS GENERATOR...
                    continue                                                                        # IF SO... GET THE NEXT GENERATOR
                if g_key in online_gens:                                                            # IF A GENERATOR...
                    g_idx = gen_keyidx[g_key]                                                       # GET GENERATOR INDEX
                if g_key in swshkeys:                                                               # IF A SWSHUNT...
                    g_idx = swsh_keyidx[g_key]                                                      # GET SWSHUNT INDEX
                base_pgen = base_gen_data[g_key][0]                                                 # GET THIS GENERATOR'S BASECASE PGEN
                pgen = c_net.res_gen.loc[g_idx, 'p_mw']                                             # GET THIS GENERATOR'S OPF PGEN

                # -- SET GEN PMAX OR PMIN -------------------------------------
                # -- DELTA P THRESHOLD FOR GENS AND BRANCHES ------------------                     # TODO... seems %outage gen is best for generators
                # if pgen - base_pgen > gpdelta_threshold:                                          # IF THIS GENERATOR INCREASED P MORE THAN THIS...
                #     c_net.gen.loc[g_idx, 'min_p_mw'] = pgen + 1e-6                                # SET THIS GENERATOR'S MINIMUM P
                # elif pgen - base_pgen < -gpdelta_threshold:                                       # IF THIS GENERATOR DECREASED P LESS THAN THIS...
                #     c_net.gen.loc[g_idx, 'max_p_mw'] = pgen - 1e-6                                # SET THIS GENERATOR'S MAXIMUM P

                # -- %OUTAGED GEN FOR GENS, DELTA P THRESHOLD FOR BRANCHES ----
                if o_key in online_gens:                                                            # IF THIS OUTAGE IS A GENERATOR...
                    outaged_pgen = base_gen_data[o_key][0]                                          # GET THE PRE-OUTAGE PGEN
                    if pgen - base_pgen > 0.01 * outaged_pgen:                                      # IF THIS GENERATOR INCREASED MORE THAN THIS...
                        c_net.gen.loc[g_idx, 'min_p_mw'] = pgen                                     # SET THIS GENERATOR'S MINIMUM POWER OUTPUT
                    elif pgen - base_pgen < -0.01 * outaged_pgen:                                   # IF THIS GENERATOR DECREASED MORE THAN THIS...
                        c_net.gen.loc[g_idx, 'max_p_mw'] = pgen                                     # SET THIS GENERATOR'S MAXIMUM POWER OUTPUT
                else:                                                                               # IF THIS OUTAGE IS A BRANCH...
                    if pgen - base_pgen > gpdelta_threshold:                                        # IF THIS GENERATOR INCREASED MORE THAN THIS...
                        c_net.gen.loc[g_idx, 'min_p_mw'] = pgen                                     # SET THIS GENERATOR'S MINIMUM POWER OUTPUT
                    elif pgen - base_pgen < -gpdelta_threshold:                                     # IF THIS GENERATOR DECREASED LESS THAN THIS...
                        c_net.gen.loc[g_idx, 'max_p_mw'] = pgen                                     # SET THIS GENERATOR'S MAXIMUM POWER OUTPUT
            step += 1                                                                               # INCREMENT ITERATOR
            processed_outages += o_keys                                                             # UPDATE THE PROCESSED OUTAGES
            elapsed_time = time.time() - master_start_time                                          # GET THE ELAPSED TIME SO FAR
            countdown_time = MaxRunningTime - elapsed_time - opf_time - finalize_time               # CALCULATE TIME LEFT

        # :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        elif len(o_keys) > 1:                                                                         # IF MORE THAN ONE OUTAGE IN GROUP... RUN IN PARALLEL
            arglist = [[c_net, x, online_gens, gen_keyidx, line_keyidx, xfmr_keyidx,                # GET ARGUMENT LIST FOR EACH PROCESS
                        genbus_dict, swingbus, swsh_keyidx, swshbus_dict, swshkeys,                 # GET ARGUMENT LIST FOR EACH PROCESS
                        ext_grid_idx, step] for x in o_keys]                                        # GET ARGUMENT LIST FOR EACH PROCESS
            results = parallel_run_outage_opf(arglist)                                              # GET PARALLEL RESULTS {swsh_gkey: [pgen, gvreg, qgen]}
            for opf_gendata_dict_i in results:                                                      # LOOP ACROSS THE OUTAGES OPF GENERATOR DICTS
                opf_gendata_dict.update(opf_gendata_dict_i)                                         # UPDATE THE MASTER OPF GENERATOR DICT

            # -- INITIALIZE GENERATOR P AND VREG CHANGE DICTS -----------------
            swsh_gkeys = swshkeys + online_gens                                                     # COMBINE SWSHUNT AND GENERATOR KEYS
            for g_key in swsh_gkeys:                                                                # LOOP ACROSS GENERATORS AND SWSHUNTS
                if g_key in online_gens:                                                            # IF A GENERATOR...
                    g_idx = gen_keyidx[g_key]                                                       # GET GENERATOR INDEX
                if g_key in swshkeys:                                                               # IF A SWSHUNT...
                    g_idx = swsh_keyidx[g_key]                                                      # GET SWSHUNT INDEX
                base_pgen = base_gen_data[g_key][0]                                                 # GET THIS GEN OR SWSHUNTS BASECASE PGEN (ZERO IF SWSHUNT)
                base_gvreg = base_gen_data[g_key][1]                                                # GET THIS GEN OR SWSHUNTS BASECASE VREG
                base_qgen = base_gen_data[g_key][2]                                                 # GET THIS GEN OR SWSHUNTS BASECASE QGEN
                pgen_minmax_dict.update({g_key: [0.0, 0.0, 0.0, base_pgen, g_idx]})                 # INITIALIZE GEN OR SWSHUNT MIN-MAX P CHANGE DICT [deltapgen,pgengneg,pgenpos,basepgen,gidx]
                vreg_minmax_dict.update({g_key: [0.0, 0.0, 0.0, base_gvreg, g_idx]})                # INITIALIZE GEN OR SWSHUNT MIN-MAX VREG CHANGE DICT [deltavreg,vregneg,vregpos,basevreg,gidx]
                qgen_minmax_dict.update({g_idx: [0.0, 0.0, base_qgen]})                             # INITIALIZE GEN OR SWSHUNT MIN-MAX Q CHANGE DICT [qmin,qmax,baseqgen]

            # == DETERMINE HOW MUCH THE GENERATORS CHANGED ====================
            solved = True
            for o_key in opf_gendata_dict:                                                          # LOOP THROUGH THE OVERLOADED OUTAGES FROM OPF RESULTS
                if not opf_gendata_dict[o_key]:                                                     # EMPTY LIST INDICATES NOSOLVE
                    print('NOSLOVE FOUND IN PARALLEL OPF ...........................', o_key)       # PRINT STATEMENT
                    c_net = last_known_good_net                                                     # SINCE NOSOLVE FOUND... LOAD LAST KNOWN GOOD CNET
                    elapsed_time = time.time() - master_start_time                                  # GET THE ELAPSED TIME SO FAR
                    countdown_time = MaxRunningTime - elapsed_time - opf_time - finalize_time       # CALCULATE TIME LEFT
                    step += 1                                                                       # INCREMENT ITERATOR
                    nosolve_outages.append(o_key)                                                   # ADD OUTAGE TO NOSOLVES
                    processed_outages.append(o_key)                                                 # UPDATE THE PROCESSED OUTAGES
                    solved = False                                                                  # SET SOLVED FLAG=FALSE
                    break                                                                           # BREAK AND PROCESS NEXT GROUP OF OUTAGES

                for g_key in opf_gendata_dict[o_key]:                                               # LOOP ACROSS THE ONLINE GENERATORS
                    if g_key == o_key:                                                              # CHECK IF THE OUTAGE WAS THIS GENERATOR...
                        continue                                                                    # IF SO... GET THE NEXT GENERATOR
                    g_idx = pgen_minmax_dict[g_key][4]                                              # GET ONLINE GENERATOR INDEX
                    base_pgen = pgen_minmax_dict[g_key][3]                                          # GET THIS GENERATOR'S BASECASE PGEN
                    pgen = opf_gendata_dict[o_key][g_key][0]                                        # GET THIS GENERATOR'S OPF PGEN
                    gpdelta = pgen - base_pgen                                                      # CALCULATE GENERATOR P CHANGE (COMPARED TO BASECASE)
                    if gpdelta < pgen_minmax_dict[g_key][1]:                                        # IF THIS GENERATOR DECREASE MORE THAN LARGEST DECREASE...
                        pgen_minmax_dict[g_key][1] = gpdelta                                        # SET THIS GENERATORS LARGEST DECREASE
                    if gpdelta > pgen_minmax_dict[g_key][2]:                                        # IF THIS GENERATOR INCREASED MORE THAN LARGEST INCREASE...
                        pgen_minmax_dict[g_key][2] = gpdelta                                        # SET THIS GENERATORS LARGEST INCREASE
                    gdelta_neg = pgen_minmax_dict[g_key][1]                                         # GET THIS LOOPS GENERATION DECREASE
                    gdelta_pos = pgen_minmax_dict[g_key][2]                                         # GET THIS LOOPS GENERATION INCREASE
                    pgen_minmax_dict[g_key][0] = max(-gdelta_neg, gdelta_pos)                       # SET SORTING VALUE TO MAX OF -DECREASE OR INCREASE

                    base_gvreg = vreg_minmax_dict[g_key][3]                                         # GET THIS GENERATOR'S BASECASE PGEN
                    gvreg = opf_gendata_dict[o_key][g_key][1]                                       # GET THIS GENERATOR'S OPF PGEN
                    gvregdelta = gvreg - base_gvreg
                    if gvregdelta < vreg_minmax_dict[g_key][1]:                                     # IF THIS GENERATOR DECREASE MORE THAN LARGEST DECREASE...
                        vreg_minmax_dict[g_key][1] = gvregdelta                                     # SET THIS GENERATORS LARGEST DECREASE
                    if gvregdelta > vreg_minmax_dict[g_key][2]:                                     # IF THIS GENERATOR INCREASED MORE THAN LARGEST INCREASE...
                        vreg_minmax_dict[g_key][2] = gvregdelta                                     # SET THIS GENERATORS LARGEST INCREASE
                    gvregdelta_neg = vreg_minmax_dict[g_key][1]                                     # GET THIS LOOPS GENERATION DECREASE
                    gvregdelta_pos = vreg_minmax_dict[g_key][2]                                     # GET THIS LOOPS GENERATION INCREASE
                    vreg_minmax_dict[g_key][0] = max(-gvregdelta_neg, gvregdelta_pos)               # SET SORTING VALUE TO MAX OF -DECREASE OR INCREASE

                    base_qgen = qgen_minmax_dict[g_idx][2]                                          # GET THIS GENERATOR'S BASECASE QGEN
                    qgen = opf_gendata_dict[o_key][g_key][2]                                        # GET THIS GENERATOR'S OPF QGEN
                    if qgen < qgen_minmax_dict[g_idx][0]:                                           # IF THIS GENERATOR Q IS LESS THAN SMALLEST SO FAR...
                        qgen_minmax_dict[g_idx][0] = qgen                                           # SET THIS GENERATORS MIN Q
                    if qgen > qgen_minmax_dict[g_idx][1]:                                           # IF THIS GENERATOR Q IS GREATER THAN LARGEST SO FAR...
                        qgen_minmax_dict[g_idx][1] = qgen                                           # SET THIS GENERATORS MAX Q
            if solved:
                # -- SINCE ALL OUTAGES SOLVED ---------------------------------
                last_known_good_net = deepcopy(c_net)                                               # SAVE LAST ITERATION CNET AS LAST KNOW GOOD NET

                # --- SET GENERATORS PGEN AND ADJUST PMIN OR PMAX ---------------------
                gpdelta_data = []                                                                   # INITIALIZE LIST FOR SORTING GENERATOR P CHANGE DATA
                for g_key in pgen_minmax_dict:                                                      # LOOP THROUGH GEN MIN-MAX CHANGE DICT
                    gpdelta_data.append(pgen_minmax_dict[g_key])                                    # ADD GEN MIN-MAX DATA TO LIST
                gpdelta_data.sort(reverse=True)                                                     # SORT LARGEST TO SMALLEST PGEN CHANGE
                gpdelta_data = [x for x in gpdelta_data if x[0] > gpdelta_threshold]                # REMOVE GENERATORS WITH INSIGNIFICANT CHANGE
                gpdelta_data = [x[1:] for x in gpdelta_data]                                        # REMOVE GDELTAS SORT VALUE
                for data in gpdelta_data:                                                           # LOOP ACROSS THE GENERATOR CHANGE DATA
                    gdelta_neg = data[0]                                                            # GET HOW MUCH THIS GENERATOR WENT DOWN (FOR THE OUTAGES RUN)
                    gdelta_pos = data[1]                                                            # GET HOW MUCH THIS GENERATOR WENT UP (FOR THE OUTAGES RUN)
                    base_pgen = data[2]                                                             # GET THIS GENERATORS PRE-OUTAGE PGEN
                    g_idx = data[3]                                                                 # GET THIS GENERATORS INDEX
                    # -- CHECK FOR PGEN CHANGE IN BOTH DIRECTIONS ---------------------
                    # if gdelta_neg < -gpdelta_threshold and gdelta_pos > gpdelta_threshold:          # CHECK IF THIS GENERATOR BOTH DECREASED AND INCREASED PGEN
                    #     pgen_ave = base_pgen + (gdelta_neg + gdelta_pos) / 2                        # CALCULATE THE AVERAGE PGEN
                    #     c_net.gen.loc[g_idx, 'p_mw'] = pgen_ave                                     # SET THIS GENERATORS PGEN
                    #     c_net.gen.loc[g_idx, 'min_p_mw'] = base_pgen + gdelta_neg                   # SET THIS GENERATORS MINPGEN
                    #     c_net.gen.loc[g_idx, 'max_p_mw'] = base_pgen + gdelta_pos                   # SET THIS GENERATORS MAXPGEN
                    # -- CHECK FOR PGEN CHANGE DOWN ONLY  -----------------------------
                    # elif gdelta_neg < -gpdelta_threshold and gdelta_pos < gpdelta_threshold:        # CHECK IF THIS GENERATOR ONLY DECREASED PGEN
                    if gdelta_neg < -gpdelta_threshold and gdelta_pos < gpdelta_threshold:          # CHECK IF THIS GENERATOR ONLY DECREASED PGEN
                        c_net.gen.loc[g_idx, 'p_mw'] = base_pgen + gdelta_neg + 1e-6                # SET THIS GENERATORS PGEN
                        c_net.gen.loc[g_idx, 'max_p_mw'] = base_pgen + gdelta_neg                   # SET THIS GENERATORS MAXPGEN
                    # -- CHECK FOR PGEN CHANGE UP ONLY  -------------------------------
                    elif gdelta_neg > -gpdelta_threshold and gdelta_pos > gpdelta_threshold:        # CHECK IF THIS GENERATOR ONLY INCREASED PGEN
                        c_net.gen.loc[g_idx, 'p_mw'] = base_pgen + gdelta_pos - 1e-6                # SET THIS GENERATORS PGEN
                        c_net.gen.loc[g_idx, 'min_p_mw'] = base_pgen + gdelta_pos                   # SET THIS GENERATORS MINPGEN

                # --- SET GEMERATORS VOLTAGE SCHEDULE AND ADJUST QMIN OR QMAX ---------
                gvregdelta_data = []                                                                # INITIALIZE LIST FOR SORTING GENERATOR VREG CHANGE DATA
                for g_key in vreg_minmax_dict:                                                      # LOOP THROUGH GEN VREG MIN-MAX CHANGE DICT
                    gvregdelta_data.append(vreg_minmax_dict[g_key])                                 # ADD GEN VREG MIN-MAX DATA TO LIST
                gvregdelta_data.sort(reverse=True)                                                  # SORT LARGEST TO SMALLEST PGEN CHANGE
                gvregdelta_data = [x for x in gvregdelta_data if x[0] > gvregdelta_threshold]       # REMOVE GENERATORS WITH INSIGNIFICANT CHANGE
                gvregdelta_data = [x[1:] for x in gvregdelta_data]                                  # REMOVE GVREG DELTAS SORT VALUE

                for data in gvregdelta_data:                                                        # LOOP ACROSS THE GENERATOR VREG CHANGE DATA
                    g_idx = data[3]                                                                 # GET THIS GENERATORS INDEX
                    gvreg_neg = data[0]                                                             # GET HOW MUCH THIS GENERATOR WENT DOWN (FOR THE OUTAGES RUN)
                    gvreg_pos = data[1]                                                             # GET HOW MUCH THIS GENERATOR WENT UP (FOR THE OUTAGES RUN)
                    base_gvreg = data[2]                                                            # GET THIS GENERATORS PRE-OUTAGE VREG
                    # -- CHECK FOR VREG CHANGE IN BOTH DIRECTIONS (SET VREG=AVE) ------
                    if gvreg_neg < -gvregdelta_threshold and gvreg_pos > gvregdelta_threshold:      # CHECK IF THIS GENERATOR BOTH DECREASED AND INCREASED VREG
                        gvreg_ave = base_gvreg + (gvreg_neg + gvreg_pos) / 2                        # CALCULATE THE AVERAGE VREG
                        c_net.gen.loc[g_idx, 'vm_pu'] = gvreg_ave                                   # SET THIS GENERATORS VREG
                        q_ave = (qgen_minmax_dict[g_idx][0] + qgen_minmax_dict[g_idx][1]) / 2.0     # CALCULATE Q AVERAGE
                        c_net.gen.loc[g_idx, 'min_q_mvar'] = qgen_minmax_dict[g_idx][0]             # SET THIS GENERATORS QMIN
                        c_net.gen.loc[g_idx, 'max_q_mvar'] = qgen_minmax_dict[g_idx][1]             # SET THIS GENERATORS QMAX
                        c_net.gen.loc[g_idx, 'q_mvar'] = q_ave                                      # SET THIS GENERATORS VREG
                    # -- CHECK FOR VREG CHANGE DOWN ONLY (SET VREG=MIN) ---------------
                    elif gvreg_neg < -gvregdelta_threshold and gvreg_pos < gvregdelta_threshold:    # CHECK IF THIS GENERATOR ONLY DECREASED VREG
                        c_net.gen.loc[g_idx, 'vm_pu'] = base_gvreg + gvreg_neg + 0.0005             # SET THIS GENERATORS VREG
                        c_net.gen.loc[g_idx, 'max_q_mvar'] = qgen_minmax_dict[g_idx][1] - 1e-6      # SET THIS GENERATORS QMAX
                    # -- CHECK FOR VREG CHANGE UP ONLY (SET VREG=MAX) -----------------
                    elif gvreg_neg > -gvregdelta_threshold and gvreg_pos > gvregdelta_threshold:    # CHECK IF THIS GENERATOR ONLY INCREASED VREG
                        c_net.gen.loc[g_idx, 'vm_pu'] = base_gvreg + gvreg_pos - 0.0005             # SET THIS GENERATORS VREG
                        c_net.gen.loc[g_idx, 'min_q_mvar'] = qgen_minmax_dict[g_idx][0] + 1e-6      # SET THIS GENERATORS QMIN

                # pp.runpp(c_net, enforce_q_lims=True)                                                # SOLVE THIS MASTER BASECASE
                # try:
                #     pp.runopp(c_net, init='pf')                                                     # RUN OPF ON THIS NEW BASECASE NETWORK
                # except:
                #     print('GROUP DID NOT SOLVE .... GET NEXT GROUP OF CONTINGENCIES')
                #     processed_outages += o_keys                                                     # UPDATE THE PROCESSED OUTAGES
                #     nosolve_outages += o_keys
                #     elapsed_time = time.time() - master_start_time                                  # GET THE ELAPSED TIME SO FAR
                #     countdown_time = MaxRunningTime - elapsed_time - opf_time - finalize_time       # CALCULATE TIME LEFT
                #     step += 1                                                                       # INCREMENT ITERATOR
                #     c_net = deepcopy(last_known_good_net)
                #     continue
                # c_net = copy_opf_to_network(c_net, c_net, gen_keyidx, genbus_dict, swingbus,        #
                #                             swsh_keyidx, swshbus_dict, ext_grid_idx)                # COPY OPF RESULTS TO THIS NETWORK
                processed_outages += o_keys                                                         # UPDATE THE PROCESSED OUTAGES
                elapsed_time = time.time() - master_start_time                                      # GET THE ELAPSED TIME SO FAR
                countdown_time = MaxRunningTime - elapsed_time - opf_time - finalize_time           # CALCULATE TIME LEFT
                step += 1                                                                           # INCREMENT ITERATOR

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    # -- END OF WHILE LOOP ------------------------------------------------------------------------
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

    # =============================================================================================
    # -- FINALIZE THE SCOPF BASECASE --------------------------------------------------------------
    # =============================================================================================
    print()
    print('---------------- RUNNING OPF ON FINAL SCOPF BASECASE ---------------')                   # PRINT MESSAGE
    if overloaded_outages:                                                                          # IF NOT ALL OVERLOADS PROCESSED...
        print('ITERATIONS TIMED OUT  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')               # PRINT MESSAGE
    a_net = deepcopy(c_net)                                                                         # COPY LAST OPF BASECASE NETWORK TO FINAL SCOPF BASECASE
    pp.runpp(a_net, enforce_q_lims=True)                                                            # SOLVE THIS FINAL BASECASE
    try:
        pp.runopp(a_net, init='pf')                                                                 # RUN OPF ON THIS NETWORK
    except:                                                                                         # IF NO SOLUTION...
        print('FINAL BASECASE DID NOT SOLVE WITH OPF... GETTING LAST KNOWN GOOD')                   # PRINT STATEMENT
        a_net = deepcopy(last_known_good_net)                                                       # GET LAST KNOWN GOOD BASECASE NETWORK
        pp.runpp(a_net, enforce_q_lims=True)                                                        # SOLVE THIS FINAL BASECASE
        pp.runopp(a_net, init='pf')                                                                 # RUN OPF ON THIS NETWORK

    net_a = copy_opf_to_network(a_net, net_a, gen_keyidx, genbus_dict, swingbus, swsh_keyidx, swshbus_dict, ext_grid_idx)   # <---- THIS IS THE FINAL SCOPF BASECASE
    pp.runpp(net_a, enforce_q_lims=True)                                                                                    # SOLVE THIS FINAL BASECASE

    min_busvoltage, max_busvoltage = get_minmax_voltage(net_a)                                      # GET MIN-MAX BASECASE BUS VOLTAGES
    min_busvoltage = round(min_busvoltage, 5)                                                       # FORMAT MIN VOLTAGE
    max_busvoltage = round(max_busvoltage, 5)                                                       # FORMAT MAX VOLTAGE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER
    max_basecase_loading = get_maxloading(net_a, line_keyidx, xfmr_keyidx)                          # GET MAX BASECASE BRANCH LOADING
    print('MAX BASECASE LOADING =', max_basecase_loading)                                           # PRINT MAX BASECASE BRANCH LOADING
    print('MIN - MAX BASECASE VOLTAGE =', [round(min_busvoltage, 5), round(max_busvoltage, 5)])     # PRINT MIN-MAX BASECASE VOLTAGES
    print('EXT_PGEN - EXT_QGEN =', [round(ex_pgen, 4), round(ex_qgen, 4)])                          # PRINT EXTERNAL GRID REAL AND REACTIVE POWER
    print('BASECASE SCOPF NETWORK CREATED .....................................', round(time.time() - scopf_start_time, 3), 'sec')

    # =============================================================================================
    # -- FINE-TUNE FINAL SCOPF BASECASE -----------------------------------------------------------
    # =============================================================================================
    print()                                                                                                                     # PRINT BLANK LINE
    print('-------------------- FINE-TUNING SCOPF BASECASE --------------------')                                               # PRINT STATEMENT
    finetune_time = time.time()                                                                                                 # INITIALIZE START TIME
    net_a = finetune_network(net_a, online_gens, gen_keyidx, genidx_dict, swshidx_dict, ext_grid_idx)                           # FINE TUNE SCOPF NETWORK
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                                                      # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                                                    # GET EXTERNAL GRID REACTIVE POWER
    base_cost = get_generation_cost(net_a, participating_gens, gen_keyidx, pwlcost_dict0)                                       # GET TOTAL COST OF GENERATION
    maxloading = get_maxloading(net_a, line_keyidx, xfmr_keyidx)                                                                # GET MAX BRANCH LOADING
    minv, maxv = get_minmax_voltage(net_a)                                                                                      # GET MIN,MAX BUS VOLTAGE
    print('FINAL SCOPF NETWORK CREATED ........................................', round(time.time() - finetune_time, 3))        # PRINT STATEMENT
    print('GENERATION COST ....................................................', '$ {0:.2f}'.format(base_cost))                # PRINT STATEMENT
    print('MAX BRANCH LOADING .................................................', maxloading)                                   # PRINT STATEMENT
    print('MIN BUS VOLTAGE ....................................................', '{0:.4f} pu'.format(minv))                    # PRINT STATEMENT
    print('MAX BUS VOLTAGE ....................................................', '{0:.4f} pu'.format(maxv))                    # PRINT STATEMENT

    # =============================================================================================
    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    # =============================================================================================
    print()
    print('WRITING BASECASE RESULTS TO FILE ... {0:8.5f} MW {1:8.5f} MVAR .....'.format(ex_pgen + 0.0, ex_qgen + 0.0))          # PRINT STATEMENT
    bus_results = deepcopy(net_a.res_bus)                                                                                       # GET BASECASE BUS RESULTS
    gen_results = deepcopy(net_a.res_gen)                                                                                       # GET BASECASE GENERATOR RESULTS
    write_bus_results(outfname, bus_results, swshidx_dict, gen_results, ext_grid_idx)                                           # WRITE SOLUTION1 BUS RESULTS
    write_gen_results(outfname, gen_results, Gids, genbuses, swshidxs)                                                          # WRITE SOLUTION1 GEN RESULTS
    print('DONE ---------------------------------------------------------------')                                               # PRINT STATEMENT
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - master_start_time, 3))    # PRINT STATEMENT

    # -- TODO... DEVELOPEMENT, COPY FILES FOR EVALUATION ------------------------------------------
    if not sys.argv[1:]:
        import shutil
        dirname = os.path.dirname(__file__)
        shutil.copy(outfname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(raw_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(con_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(inl_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(rop_fname, os.path.join(dirname, 'GitHub_Work'))
        shutil.copy(os.path.realpath(__file__), os.path.join(dirname, 'GitHub_Work/MyPython1.py'))
