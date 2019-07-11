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

cwd = os.path.dirname(__file__)
print()

# -----------------------------------------------------------------------------
# -- USING COMMAND LINE -------------------------------------------------------
# -----------------------------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
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

    outfname = cwd + '//solution1.txt'
    try:
        os.remove(outfname)
    except FileNotFoundError:
        pass
    print('===================  {0:14s}  {1:10s}  ==================='.format(network, scenario))

SwVreg_Custom = 1.040       # SWING GENERATORS INITIAL USER DEFINED VOLTAGE SCHEDULE
Gvreg_Custom = 1.03         # NON-SWING GENERATORS INITIAL USER DEFINED VOLTAGE SCHEDULE
SwShVreg_Custom = 1.03      # SWITCHED SHUNTS INITIAL USER DEFINED VOLTAGE SCHEDULE
SWVREG = 0                  # SWING GENERATORS VOLTAGE SCHEDULE ... 0=DEFAULT_GENV_RAW, 1=CUSTOM)
GVREG = 0                   # NON-SWING GENERATORS VOLTAGE SCHEDULES ... 0=DEFAULT_GENV_RAW, 1=CUSTOM(ALL)
SWSHVREG = 0                # SWITCHED SHUNTS VOLTAGE SCHEDULES ........ 0=DEFAULT_RAW, 1=CUSTOM(ALL)
MaxLoading = 95.0           # MAXIMUM %BRANCH LOADING FOR N-0 AND N-1
MaxMinBusVoltageAdj = 0.015
MaxRunningTime = 600.0


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


def copy_opf_to_network(netx, gendict, genbusdict, swbus, swshdict, swshbusdict):
    """copy opf results to this network"""
    netx.gen['p_mw'] = netx.res_gen['p_mw']                                                         # set this network generators power to opf results
    for g_key in gendict:                                                                           # loop across generator keys
        g_idx = gendict[g_key]                                                                      # get generator index
        gen_bus = genbusdict[g_idx]                                                                 # get generator bus
        if gen_bus == swbus:                                                                        # check if swing bus...
            continue                                                                                # if swing bus, get next generator
        netx.gen.loc[g_idx, 'vm_pu'] = netx.res_bus.loc[gen_bus, 'vm_pu']                           # set this network gens vreg to opf results
    for sh_key in swshdict:                                                                         # loop across swshunt keys
        sh_idx = swshdict[sh_key]                                                                   # get swshunt index
        sh_bus = swshbusdict[sh_idx]                                                                # get swshunt bus
        netx.gen.loc[sh_idx, 'vm_pu'] = netx.res_bus.loc[sh_bus, 'vm_pu']                           # set this network swshunt vreg to opf results
    return netx


def estimate_swing_vreg(netx, nlevelbuses, swingbusidx, swinggenidxs, extgrididx):
    """get better estimate for swing generator(s) voltage schedule"""
    nlevelbuses_v = [x for x in netx.res_bus.loc[nlevelbuses, 'vm_pu']]                             # get list of nlevel bus voltages
    max_v = max(nlevelbuses_v)                                                                      # get max voltage of nlevel buses
    sw_maxv = netx.bus.loc[swingbusidx, 'max_vm_pu']                                                # get max voltage of swing bus
    sw_setpoint = min(max_v, sw_maxv)                                                               # determine swing bus voltage setpoint
    netx.gen.loc[swinggenidxs, 'vm_pu'] = sw_setpoint                                               # set swing gens vreg = max of nlevel buses voltage
    netx.ext_grid.loc[extgrididx, 'vm_pu'] = sw_setpoint                                            # set extgrid vreg = max of nlevel buses voltage
    return netx


def get_dominant_outages(xnet, goutage_keys, boutage_keys, onlinegens, gendict, linedict, xfmrdict, swinggenidxs, iteration, lineratedict, xfmrratedict, finalflag):
    """get dominant outages resulting in branch loading"""
    nosolves_found = False
    nosolve_keys = []
    swinggen_in_outages = False
    swinggen_key = ''
    contrained_line_dict = {}                                                                       # initialize dict
    contrained_xfmr_dict = {}                                                                       # initialize dict
    for line_key in line_dict:                                                                      # loop through lines
        contrained_line_dict.update({line_key: []})                                                 # initialize dict
    for xfmr_key in xfmr_dict:                                                                      # loop through xfmrs
        contrained_xfmr_dict.update({xfmr_key: []})                                                 # initialize dict
    outage_keys = goutage_keys + boutage_keys                                                       # combine generator and branch keys
    for o_key in outage_keys:                                                                       # loop across outages
        net = copy.deepcopy(xnet)                                                                   # get fresh copy of network
        if o_key in onlinegens:                                                                     # check if a generator...
            g_idx = gendict[o_key]                                                                  # get generator index
            net.gen.in_service[g_idx] = False                                                       # switch off outaged generator
            if g_idx in swinggenidxs and len(swinggenidxs) == 1 and iteration == 0:                 # check if outage is the only swing generator
                swinggen_in_outages = True
                swinggen_key = o_key
                break
        elif o_key in linedict:                                                                     # check if a line...
            line_idx = linedict[o_key]                                                              # get line index
            net.line.in_service[line_idx] = False                                                   # switch out outaged line
        elif o_key in xfmrdict:                                                                     # check if a xfmr...
            xfmr_idx = xfmrdict[o_key]                                                              # get xfmr index
            net.trafo.in_service[xfmr_idx] = False                                                  # switch out outaged xfmr
        try:                                                                                        # try straight powerflow solution
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow
        except:                                                                                     # if no solution...
            nosolve_keys.append(o_key)
            nosolves_found = True
            continue                                                                                # get next contingency
        for line_key in linedict:                                                                   # loop across line keys
            line_idx = linedict[line_key]                                                           # get line index
            lineloading = net.res_line.loc[line_idx, 'loading_percent']                             # get this line loading
            if lineloading > 100.0:                                                                 # if loading greater than 100%...
                mva_overloading = lineratedict[line_key] * (lineloading - 100.0) / 100.0            # calculate mva overloading
                contrained_line_dict[line_key].append([mva_overloading, o_key])                     # add mva overloading and outagekey to dict
        for xfmr_key in xfmrdict:                                                                   # loop across xfmr keys
            xfmr_idx = xfmrdict[xfmr_key]                                                           # get xfmr index
            xfmrloading = net.res_trafo.loc[xfmr_idx, 'loading_percent']                            # get this xfmr loading
            if xfmrloading > 100.0:                                                                 # if loading greater than 100%...
                mva_overloading = xfmrratedict[xfmr_key] * (xfmrloading - 100.0) / 100.0            # calculate mva overloading
                contrained_xfmr_dict[xfmrkey].append([mva_overloading, o_key])                      # add mva overloading and outagekey to dict
    tempdict = {}                                                                                   # initialize tempdict
    for lkey in contrained_line_dict:                                                               # loop across constrained line dict
        if contrained_line_dict[lkey]:                                                              # if dict list value is not empty...
            contrained_line_dict[lkey].sort(reverse=True)                                           # sort list and add to tempdict
            tempdict.update({lkey: contrained_line_dict[lkey][0]})                                  # add key:[list] to tempdict
    contrained_line_dict = tempdict                                                                 # reassign dict
    tempdict = {}                                                                                   # initialize tempdict
    for xkey in contrained_xfmr_dict:                                                               # loop across constrained xfmr dict
        if contrained_xfmr_dict[xkey]:                                                              # if dict list value is not empty...
            contrained_xfmr_dict[xkey].sort(reverse=True)                                           # sort list and add to tempdict
            tempdict.update({xkey: contrained_xfmr_dict[xkey][0]})                                  # add key:[list] to tempdict
    contrained_xfmr_dict = tempdict                                                                 # reassign dict
    dominantoutages = []                                                                            # initialize list
    for lkey in contrained_line_dict:                                                               # loop across constrained line dict
        dominantoutages.append(contrained_line_dict[lkey])                                          # add [maxloading, outagekey] fot list
    for xkey in contrained_xfmr_dict:                                                               # loop across constrained xfmr dict
        dominantoutages.append(contrained_xfmr_dict[xkey])                                          # add [maxloading, outagekey] fot list
    dominantoutages.sort(reverse=True)                                                              # sort list high-low
    templist = []                                                                                   # initialize list
    overloadlist = []                                                                               # initialize list
    for outage in dominantoutages:                                                                  # loop across dominant outages list
        if outage[1] not in templist:                                                               # check if outagekey not in the list...
            overloadlist.append(round(outage[0], 1))                                                # add overloading to list
            templist.append(outage[1])                                                              # add outgagekey to templist
    dominantoutages = templist                                                                      # reassign list
    totaloverloading = sum(overloadlist)
    if not finalflag:
        if swinggen_in_outages and iteration == 0:
            dominantoutages = [swinggen_key]
            totaloverloading = 999.9
            print('DOMINANT OUTAGES{0:d} ({1:d})'.format(iteration, 1), [swinggen_key], '<-- SWING GENERATOR IN OUTAGES... RUN IT FIRST')
        elif nosolves_found:
            print('DOMINANT OUTAGES{0:d} ({1:d})'.format(iteration, len(overloadlist)), dominantoutages, overloadlist, '({0:.1f} MVA)'.format(totaloverloading), 'NOSOLVES:', nosolve_keys)
        else:
            print('DOMINANT OUTAGES{0:d} ({1:d})'.format(iteration, len(overloadlist)), dominantoutages, overloadlist, '({0:.1f} MVA)'.format(totaloverloading))
    else:
        print('REMAINING N-1 OVERLOADS = {0:d}'.format(len(overloadlist)))
        print('N-1 OUTAGES WITH OVERLOADS =', dominantoutages)
        print('TOTAL N-1 OVERLOADING = {0:.1f} MVA'.format(totaloverloading))
    return dominantoutages, totaloverloading


def get_base_pgens(xnet, onlinegens, gendict, genbusdict, swbus):
    """get generators pgen for this network"""
    basepgendict = {}                                                                               # initialize dict
    for g_key in onlinegens:                                                                        # loop across online generators
        g_idx = gendict[g_key]                                                                      # get generator index
        gen_bus = genbusdict[g_idx]                                                                 # get generator bus
        if gen_bus == swbus:                                                                        # check if swing bus...
            continue                                                                                # if swing bus, get next generator
        pgen = round(xnet.gen.loc[g_idx, 'p_mw'], 1)                                                # get generator's regulated voltage
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


def get_maxloading(xnet):
    """get max line or xfmr loading for this network"""
    line_loading = max(xnet.res_line['loading_percent'].values)                                     # get max line loading
    xfmr_loading = max(xnet.res_trafo['loading_percent'].values)                                    # get max xfmr loading
    max_loading = max(line_loading, xfmr_loading)                                                   # get max of maxs
    max_loading = round(max_loading, 3)
    return max_loading


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
    master_start_time = time.time()

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
        pp.create_bus(net_a, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.nvhi-MaxMinBusVoltageAdj, min_vm_pu=bus.nvlo+MaxMinBusVoltageAdj, in_service=True, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        idx = pp.create_bus(net_c, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.evhi-MaxMinBusVoltageAdj, min_vm_pu=bus.evlo+MaxMinBusVoltageAdj, in_service=True, index=busnum)
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
    # loads = (load.i, load.id, load.status, load.pl, load.ql)
    for load in raw_data.raw.loads.values():
        status = bool(load.status)
        if not status:
            continue
        loadbus = load.i
        loadid = load.id
        loadname = str(loadbus) + '-' + loadid
        loadp = load.pl
        loadq = load.ql
        loadmva = math.sqrt(loadp ** 2 + loadq ** 2)
        if loadp < 0.0:
            pp.create_sgen(net_a, loadbus, p_mw=-loadp, q_mvar=-loadq, sn_mva=loadmva, name=loadname)
            pp.create_sgen(net_c, loadbus, p_mw=-loadp, q_mvar=-loadq, sn_mva=loadmva, name=loadname)
        else:
            pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)
            pp.create_load(net_c, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    Gids = []
    gen_dict = {}
    genidx_dict = {}
    swinggen_idxs = []
    genarea_dict = {}
    genidxs = []
    genbus_dict = {}
    gen_minmax_dict = {}
    participating_gens = []
    area_participating_gens = {}
    for area in areas:
        area_participating_gens.update({area: []})
        
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
        if SWVREG == 1:
            vreg = SwVreg_Custom
        status = swgen_data[9]
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        power_factor = pmax / genmva
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25,  cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status, index=idx, slack=False)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
                area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='SWGEN', controllable=False, in_service=status, index=idx)
        swing_vreg = vreg
        swinggen_idxs.append(idx)
        gen_dict.update({genkey: idx})
        genbuses.append(genbus)
        Gids.append("'" + gid + "'")
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genidx_dict.update({genbus: idx})
        genbus_dict.update({idx: genbus})
        gen_minmax_dict.update({genkey: [pmin, pmax]})

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
        power_factor = pmax / genmva
        if GVREG:
            vreg = Gvreg_Custom
        status = bool(gen.stat)
        pcostdata = None
        genkey = str(genbus) + '-' + str(gid)
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='GEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=True, in_service=status, index=idx)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
                area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status, index=idx)
        Gids.append("'" + gid + "'")
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        gen_dict.update({genkey: idx})
        genbus_dict.update({idx: genbus})
        gen_minmax_dict.update({genkey: [pmin, pmax]})

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
            pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxshidx_dict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
    swsh_dict = {}
    swshbus_dict = {}
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
            if SWSHVREG:
                vreg = SwShVreg_Custom
            if shuntbus in genbuses:
                gidx = genidx_dict[shuntbus]
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
                                rdss_pu=99.99, xdss_pu=99.99, cos_phi=1e-6, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, shuntbus, pgen, vm_pu=vreg, sn_mva=shuntmva, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=total_qmax, min_q_mvar=total_qmin,
                          rdss_pu=99.9, xdss_pu=99.99, cos_phi=1e-6, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH', index=idx)
            swshidx_dict.update({shuntbus: idx})
            swshidxs.append(idx)
            swsh_dict.update({swshkey: idx})
            swshbus_dict.update({idx: shuntbus})
            area_swhunts[busarea_dict[shuntbus]].append(swshkey)

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st, line.len, line.met)
    line_dict = {}
    line_ratea_dict = {}
    line_ratec_dict = {}
    lineidxs = []
    branch_areas = {}
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
        pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey, max_loading_percent=MaxLoading, in_service=status, index=idx)
        line_dict.update({linekey: idx})
        lineidxs.append(idx)
        line_ratea_dict.update({linekey: base_mva_rating})
        line_ratec_dict.update({linekey: mva_rating})
        branch_areas.update({linekey: [busarea_dict[frombus], busarea_dict[tobus]]})

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.nomv1,
    #           xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.nomv2, xfmr.stat)
    xfmr_dict = {}
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
        pp.create_transformer_from_parameters(net_c, highbus, lowbus, xfmr.ratc1, highkv, lowkv, r_pct_c, z_pct_c, pfe_kw=0.0, i0_percent=0.0,
                                              shift_degree=xfmr.ang1, tap_side=tapside, tap_neutral=tapneutral, tap_max=tapmax, tap_min=tapmin,
                                              tap_step_percent=tapsteppct, tap_pos=tappos,
                                              in_service=status, name=xfmrkey, max_loading_percent=MaxLoading, index=idx)

        xfmr_dict.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmr_ratec_dict.update({xfmrkey: xfmr.ratc1})
        xfmridxs.append(idx)
        branch_areas.update({xfmrkey: [busarea_dict[highbus], busarea_dict[lowbus]]})
    for bkey in branch_areas:
        branch_areas[bkey] = list(set(branch_areas[bkey]))

    # == ADD EXTERNAL GRID ========================================================================
    # == WITH DUMMY TIE TO RAW SWING BUS ==========================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)                                                 # CURRENT RATING USING SWING KV
    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.011, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, type='p')
    # pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=1, cq0_eur=1e6, type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
    tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', in_service=True, max_loading_percent=100.0)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.01, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, cq1_eur_per_mvar=0, cq0_eur=1e9)
    # pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=0, cq0_eur=1e9, type='q')

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 3), 'sec')

    # == MISC NETWORK DATA ========================================================================
    goutagekeys = list(outage_dict['gen'].keys())                                                   # GET OUTAGED GENERATOR KEYS
    boutagekeys = list(outage_dict['branch'].keys())                                                # GET OUTAGED BRANCH KEYS
    online_gens = []                                                                                # INITIALIZE ONLINE GENERATOR LIST
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        if not net_a.gen.loc[gidx, 'in_service']:                                                   # CHECK IF GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT GENERATOR KEY
        online_gens.append(gkey)                                                                    # ADD ONLINE GENERATOR TO LIST
    levels_out = 3                                                                                  # DEFINE BUS NLEVELS FOR ADJUSTING SWING GEN SETPOINT
    nlevel_buses = [swingbus]                                                                       # INITIALIZE BUS LIST
    for i in range(levels_out):                                                                     # LOOP THROUGH HOW MANY LEVELS...
        nlevel_buses = pp.get_connected_buses(net_a, nlevel_buses, respect_in_service=True)         # NEW BUSES ARE ADDED TO BUSLIST
    nlevel_buses = [x for x in nlevel_buses if x != ext_grid_idx]                                   # REMOVE EXTGRID BUS FROM BUSLIST

    # -- SOLVE INITIAL NETWORKS WITH STRAIGHT POWERFLOW -------------------------------------------
    solve_starttime = time.time()
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK
    print('   NETWORKS SOLVED .................................................', round(time.time() - solve_starttime, 3), 'sec')

    # -- SOLVE INITIAL NETWORKS WITH OPF ----------------------------------------------------------
    net = copy.deepcopy(net_a)
    pp.runopp(net, init='pf', enforce_q_lims=True)                                                  # RUN OPF ON THIS NETWORK
    net = copy_opf_to_network(net, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)        # COPY OPF RESULTS TO THIS NETWORK
    net_a = copy_opf_to_network(net, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)      # COPY OPF RESULTS TO THIS NETWORK
    net_c = copy_opf_to_network(net, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)      # COPY OPF RESULTS TO THIS NETWORK
    pp.runpp(net, enforce_q_lims=True)                                                              # RUN POWERFLOW
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN POWERFLOW
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN POWERFLOW

    # -- ATTEMPT AT BETTER ESTIMATE FOR SWING VREG ------------------------------------------------
    net = estimate_swing_vreg(net, nlevel_buses, swingbus_idx, swinggen_idxs, ext_grid_idx)         # SET SWING GENERATOR(S) VREG TO MAX OF NEIGHBOR BUSES
    net_a = estimate_swing_vreg(net, nlevel_buses, swingbus_idx, swinggen_idxs, ext_grid_idx)       # SET SWING GENERATOR(S) VREG TO MAX OF NEIGHBOR BUSES
    net_c = estimate_swing_vreg(net, nlevel_buses, swingbus_idx, swinggen_idxs, ext_grid_idx)       # SET SWING GENERATOR(S) VREG TO MAX OF NEIGHBOR BUSES
    pp.runpp(net, enforce_q_lims=True)                                                              # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK

    # *********************************************************************************************
    # -- FIND BASECASE SCOPF OPERATING POINT ------------------------------------------------------
    # *********************************************************************************************
    print('-------------------- ATTEMPTING BASECASE SCOPF ---------------------')
    elapsed_time = round(time.time() - master_start_time, 3)                                        # GET THE ELAPSED TIME SO FAR
    time_to_finalize = 30.0                                                                         # TODO ... GUESS AT TIME TO FINALIZE
    countdown_time = MaxRunningTime - elapsed_time - time_to_finalize                               # INITIALIZE COUNTDOWN TIME
    a_net = copy.deepcopy(net_a)                                                                    # INITIALIZE FIRST MASTER BASECASE

    # =============================================================================================
    # -- LOOP WHILE THERE ARE REMAINING DOMINANT OUTAGES ------------------------------------------
    # =============================================================================================
    step = 0                                                                                        # INITIALIZE WHILE LOOP ITERATOR
    start_time = time.time()                                                                        # SET THE WHILE LOOP START TIME
    this_iteration_time = 0.0                                                                       # INITIALIZE TIME FOR EACH WHILE LOOP ITERATION
    max_iteration_time = 0.0                                                                        # INITIALIZE MAX ITERATION TIME
    processed_outages = []                                                                          # INITIALIZE LIST OF ALREADY PROCESSED OUTAGES
    while countdown_time > 0.0:                                                                     # LOOP WHILE TIME REMAINS
        start_iteration_time = time.time()                                                          # INITIALIZE START ITERATION TIME
        pp.runpp(a_net, enforce_q_lims=True)                                                        # SOLVE THIS MASTER BASECASE
        # == GET THIS MASTER BASECASE OPERATING POINT =============================================
        base_pgen_dict = get_base_pgens(a_net, online_gens, gen_dict, genbus_dict, swingbus)        # GET GENERATORS PGEN FOR THIS MASTER BASECASE
        # == INITIALIZE N-1 NETWORK ===============================================================
        c_net = copy.deepcopy(a_net)                                                                # INITIALIZE N-1 NETWORK WITH LATEST VERSION OF N-0
        c_net.line['max_loading_percent'] = net_c.line['max_loading_percent']                       # CHANGE LINE MAXLOADING TO RATEC
        c_net.trafo['max_loading_percent'] = net_c.trafo['max_loading_percent']                     # CHANGE XFMR MAXLOADING TO RATEC
        # == GET DOMINANT OUTAGES RESULTING IN BRANCH LOADING VIOLATIONS ==========================
        max_total_overloading = 9999.9                                                              # INITIALIZE MAX TOTAL OVERLOAING FOR THIS ITERATION
        dominant_outages, total_overloading = get_dominant_outages(c_net, goutagekeys, boutagekeys, online_gens, gen_dict, line_dict, xfmr_dict, swinggen_idxs,
                                                                   step, line_ratec_dict, xfmr_ratec_dict, False)
        remaining_outages = [x for x in dominant_outages if x not in processed_outages]             # GET REMAINING OUTAGES

        if total_overloading < max_total_overloading:                                               # CHECK IF TOTAL N-1 OVERLOADING IS LESS LESS THAN MAX TOTAL OVERLOADIN...
            min_loading_net = copy.deepcopy(a_net)                                                  # IF LESS, COPY THIS MASTER BASECASE NETORK TO PLACEHOLDER NETWORK
            max_total_overloading = round(total_overloading, 1)                                     # UPDATE MAX TOTAL N-1 OVERLOADING VALUE

        # == CHECK IF NEED TO EXIT WHILE LOOP =====================================================
        if not remaining_outages or countdown_time <= max_iteration_time:                           # IF NO MORE REMAINING OUTAGES OR NEXT LOOP WILL EXCEED COUNTDOWN TIME...
            print('---------------- RUNNING OPF ON FINAL SCOPF BASECASE ---------------')           # PRINT MESSAGE
            if remaining_outages:                                                                   # IF NOT ALL OUTAGES PROCESSED...
                print('ITERATIONS TIMED OUT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')       # PRINT MESSAGE
            a_net = copy.deepcopy(min_loading_net)                                                  # COPY BEST MASTER NETWORK FOUND TO FINAL SCOPF BASECASE
            pp.runopp(a_net, init='pf', enforce_q_lims=True)                                        # RUN OPF ON THIS NETWORK
            a_net.gen['p_mw'] = a_net.res_gen['p_mw']                                                       # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
            a_net = copy_opf_to_network(a_net, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)    # COPY OPF RESULTS TO THIS NETWORK
            pp.runpp(a_net, enforce_q_lims=True)                                                            # SOLVE THIS MASTER BASECASE
            c_net = copy.deepcopy(a_net)                                                            # INITIALIZE N-1 NETWORK WITH LATEST MASTER BASECASE
            c_net.line['max_loading_percent'] = net_c.line['max_loading_percent']                   # CHANGE LINE MAXLOADING TO RATEC
            c_net.trafo['max_loading_percent'] = net_c.trafo['max_loading_percent']                 # CHANGE XFMR MAXLOADING TO RATEC
            max_basecase_loading = get_maxloading(a_net)                                            # GET MAX BASECASE BRANCH LOADING
            min_busvoltage, max_busvoltage = get_minmax_voltage(a_net)                              # GET MIN-MAX BASECASE BUS VOLTAGES
            min_busvoltage = round(min_busvoltage, 5)                                               # FORMAT MIN VOLTAGE
            max_busvoltage = round(max_busvoltage, 5)                                               # FORMAT MAX VOLTAGE
            ex_pgen = a_net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                  # GET EXTERNAL GRID REAL POWER
            ex_qgen = a_net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                # GET EXTERNAL GRID REACTIVE POWER
            print('MAX BASECASE LOADING = {0:.2f}'.format(max_basecase_loading))                    # PRINT MAX BASECASE BRANCH LOADING
            print('MIN - MAX BASECASE VOLTAGE =', [min_busvoltage, max_busvoltage])                 # PRINT MIN-MAX BASECASE VOLTAGES
            dominant_outages, total_overloading = get_dominant_outages(c_net, goutagekeys, boutagekeys, online_gens, gen_dict, line_dict, xfmr_dict, swinggen_idxs,
                                                                       step, line_ratec_dict, xfmr_ratec_dict, True)
            print('EXT_PGEN - EXT_QGEN =', [round(ex_pgen, 4), round(ex_qgen, 4)])                  # PRINT EXTERNAL GRID REAL AND REACTIVE POWER
            print('BASECASE SCOPF NETWORK CREATED .....................................', round(time.time() - start_time, 3), 'sec')
            # a_net.gen['min_p_mw'] = net_a.gen['min_p_mw']                                           # ? RESTORE BASECASE GENERATOR'S MINIMUM POWER OUTPUT
            # a_net.gen['max_p_mw'] = net_a.gen['max_p_mw']                                           # ? RESTORE BASECASE GENERATOR'S MAXIMUM POWER OUTPUT
            net_a = copy.deepcopy(a_net)                                                            # TODO <---- a_net IS THE FINAL SCOPF BASECASE
            break                                                                                   # EXIT WHILE LOOP AND FINE-TUNE BASECASE

        # == RUN WORST REMAINING DOMINANT OUTAGE ON THIS BASECASE WITH OPF ========================
        o_key = remaining_outages[0]                                                                # SET THIS WORST OUTAGE KEY
        net = copy.deepcopy(c_net)                                                                  # GET FRESH COPY OF THIS MASTER NETWORK
        if o_key in online_gens:                                                                    # CHECK IF A GENERATOR...
            g_idx = gen_dict[o_key]                                                                 # GET GENERATOR INDEX
            net.gen.in_service[g_idx] = False                                                       # SWITCH OFF OUTAGED GENERATOR
        elif o_key in line_dict:                                                                    # CHECK IF A LINE...
            line_idx = line_dict[o_key]                                                             # GET LINE INDEX
            net.line.in_service[line_idx] = False                                                   # SWITCH OUT OUTAGED LINE
        elif o_key in xfmr_dict:                                                                    # CHECK IF A XFMR...
            xfmr_idx = xfmr_dict[o_key]                                                             # GET XFMR INDEX
            net.trafo.in_service[xfmr_idx] = False                                                  # SWITCH OUT OUTAGED XFMR
        pp.runpp(net, enforce_q_lims=True)                                                          # SOLVE THIS NETWORK WITH POWERFLOW
        try:                                                                                        # TRY OPF POWERFLOW SOLUTION
            pp.runopp(net, init='pf', enforce_q_lims=True)                                          # RUN OPF ON THIS NETWORK
        except:                                                                                                             # IF NO SOLUTION...
            print('SUB-NETWORK DID NOT SOLVE WITH OPF .................................', o_key, ' SKIP, GET NEXT OUTAGE')  # PRINT NOSOLVE MESSAGE
            processed_outages.append(o_key)
            continue                                                                                # GET NEXT CONTINGENCY
        net = copy_opf_to_network(net, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)    # COPY OPF RESULTS TO THIS NETWORK
        pp.runpp(net, enforce_q_lims=True)                                                          # SOLVE THIS NETWORK WITH POWERFLOW

        # -- GET GENERATORS PGEN FROM OPF RESULTS (SET IF SIGNIFICANT CHANGE) ---------------------
        for g_key in participating_gens:                                                            # LOOP ACROSS PARTICIPATING GENERATOR KEYS
            if g_key == o_key:                                                                      # CHECK IF THIS OUTAGE IS THIS GENERATOR...
                continue                                                                            # IF SO... GET THE NEXT GENERATOR
            g_idx = gen_dict[g_key]                                                                 # GET GENERATOR INDEX
            gen_bus = genbus_dict[g_idx]                                                            # GET GENERATOR BUS
            if gen_bus == swingbus:                                                                 # CHECK IF THIS GENERATOR IS CONNECTED TO THE SWING BUS...
                continue                                                                            # IF SO... GET NEXT GENERATOR
            base_pgen = base_pgen_dict[g_key]                                                       # GET THIS GENERATOR'S BASECASE PGEN
            pgen = net.res_gen.loc[g_idx, 'p_mw']                                                   # GET THIS GENERATOR'S N-1 PGEN
            pgen_delta = abs(pgen - base_pgen)                                                      # CALCULATE THIS GENERATOR'S PGEN CHANGE (COMPARED TO BASECASE)
            if pgen_delta > 1.2:                                                                    # IF THIS GENERATOR CHANGED MORE THAN THIS... (1.2 SEEMS THE "SWEETSPOT")
                a_net.gen.loc[g_idx, 'p_mw'] = pgen                                                 # SET THIS GENERATOR'S PGEN TO OPF RESULT
                a_net.gen.loc[g_idx, 'min_p_mw'] = pgen                                             # SET THIS GENERATOR'S MAXIMUM POWER OUTPUT
                a_net.gen.loc[g_idx, 'max_p_mw'] = pgen                                             # SET THIS GENERATOR'S MINIMUM POWER OUTPUT
        processed_outages.append(o_key)                                                             # UPDATE THE PROCESSED OUTAGES
        this_iteration_time = time.time() - start_iteration_time                                    # CALCULATE THIS ITERATION TIME
        if this_iteration_time > max_iteration_time:                                                # IF THIS ITERATION TIME EXCEED MAX ITERATION TIME
            max_iteration_time = this_iteration_time                                                # UPDATE MAX ITERATION TIME
        countdown_time -= this_iteration_time                                                       # DECREMENT COUNTDOWN TIME
        step += 1                                                                                   # INCREMENT ITERATOR

    # =============================================================================================
    # -- FINE-TUNE FINAL SCOPF NETWORKS -----------------------------------------------------------
    # =============================================================================================
    print('-------------------- FINE-TUNING SCOPF BASECASE --------------------')
    start_time = time.time()

    # -- FINAL ATTEMPT AT BETTER ESTIMATE FOR SWING VREG ------------------------------------------
    net_a = estimate_swing_vreg(net_a, nlevel_buses, swingbus_idx, swinggen_idxs, ext_grid_idx)     # SET SWING GENERATOR(S) VREG TO MAX OF NEIGHBOR BUSES
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE WITH POWERFLOW

    # TODO? RUN OPF ON BASECASE TO CORRECT HIGH OR LOW BUS VOLTAGES -------------------------------
    # pp.runopp(net_a, init='pf', enforce_q_lims=True)                                                  # RUN OPF ON THIS NETWORK
    # net_a = copy_opf_to_network(net_a, gen_dict, genbus_dict, swingbus, swsh_dict, swshbus_dict)        # COPY OPF RESULTS TO THIS NETWORK
    # pp.runpp(net_a, enforce_q_lims=True)                                                              # RUN POWERFLOW

    # -- INSURE GENERATORS ARE MEETING VOLTAGE SCHEDULE -------------------------------------------
    previous_genbus = None                                                                          # IN CASE THERE IS MORE THAN ONE GENERATOR PER BUS
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        genbus = genbus_dict[gidx]                                                                  # GET GENERATOR BUS
        qgen = net_a.res_gen.loc[gidx, 'q_mvar']                                                      # THIS GENERATORS QGEN
        qmin = net_a.gen.loc[gidx, 'min_q_mvar']                                                      # THIS GENERATORS QMIN
        qmax = net_a.gen.loc[gidx, 'max_q_mvar']                                                      # THIS GENERATORS QMAX
        bus_voltage = net_a.res_bus.loc[genbus, 'vm_pu']                                              # THIS GENERATORS BUS VOLTAGE
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        if qgen == qmin or qgen == qmax:                                                            # IF THIS GENERATOR AT +/- QLIMIT...
            net_a.gen.loc[gidx, 'vm_pu'] = bus_voltage                                                # THIS NETWORK, SET THIS GENERATORS VREG TO BUS VOLTAGE
            if genbus != previous_genbus:                                                           # IF ALL GENERATORS ON THIS BUS ARE FOUND...
                pp.runpp(net_a, init='results', enforce_q_lims=True)                                  # THIS NETWORK, RUN STRAIGHT POWER FLOW
        previous_genbus = genbus

    # -- INSURE SWSHUNTS SUSCEPTANCE IS WITHIN LIMITS IN BASECASE ---------------------------------
    # -- HOPE CONSERVATIVE ENOUGH TO HOLD UP WITH CONTINGENCIES -----------------------------------
    for shkey in swsh_dict:                                                                         # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_dict[shkey]                                                                    # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        qgen = net_a.res_gen.loc[shidx, 'q_mvar']                                                     # GET SWSHUNT QGEN
        qmin = net_a.gen.loc[shidx, 'min_q_mvar']                                                     # GET MINIMUM SWSHUNT REACTIVE CAPABILITY
        qmax = net_a.gen.loc[shidx, 'max_q_mvar']                                                     # GET MAXIMUM SWSHUNT REACTIVE CAPABILITY
        voltage = net_a.res_bus.loc[shbus, 'vm_pu']                                                   # GET SWSHUNT BUS VOLTAGE
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

    while not zeroed and zstep < 20:                                                                # LIMIT WHILE LOOP ITERATIONS
        zeroed = True                                                                               # SET ZEROED FLAG = TRUE
        # -- CALCULATE PARTICIPATING GENERATORS UP-DOWN P AND Q MARGINS -------
        p_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL P-UP MARGIN
        p_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL P-DOWN MARGIN
        p_upmargin_dict = {}                                                                        # INITIALIZE P-UP MARGIN DICT
        p_downmargin_dict = {}                                                                      # INITIALIZE P-DOWN MARGIN DICT
        q_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL Q-UP MARGIN
        q_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL Q-DOWN MARGIN
        q_upmargin_dict = {}                                                                        # INITIALIZE Q-UP MARGIN DICT
        q_downmargin_dict = {}                                                                      # INITIALIZE Q-DOWN MARGIN DICT
        for gkey in participating_gens:                                                             # LOOP THROUGH PARTICIPATING GENERATORS
            gidx = gen_dict[gkey]                                                                   # GET THIS PARTICIPATING GENERATOR INDEX
            pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                    # THIS GENERATORS PGEN
            pmin = net_a.gen.loc[gidx, 'min_p_mw']                                                    # THIS GENERATORS PMIN
            pmax = net_a.gen.loc[gidx, 'max_p_mw']                                                    # THIS GENERATORS PMAX
            p_upmargin = pmax - pgen                                                                # THIS GENERATORS P-UP MARGIN
            p_downmargin = pgen - pmin                                                              # THIS GENERATORS P-DOWN MARGIN
            p_upmargin_dict.update({gidx: p_upmargin})                                              # UPDATE P-UP MARGIN DICT
            p_upmargin_total += p_upmargin                                                          # INCREMENT TOTAL P-UP MARGIN
            p_downmargin_dict.update({gidx: p_downmargin})                                          # UPDATE P-DOWN MARGIN DICT
            p_downmargin_total += p_downmargin                                                      # INCREMENT TOTAL P-DOWN MARGIN
            qgen = net_a.res_gen.loc[gidx, 'q_mvar']                                                  # THIS GENERATORS QGEN
            qmin = net_a.gen.loc[gidx, 'min_q_mvar']                                                  # THIS GENERATORS QMIN
            qmax = net_a.gen.loc[gidx, 'max_q_mvar']                                                  # THIS GENERATORS QMAX
            q_upmargin = qmax - qgen                                                                # THIS GENERATORS Q-UP MARGIN
            q_downmargin = qgen - qmin                                                              # THIS GENERATORS Q-DOWN MARGIN
            q_upmargin_dict.update({gidx: q_upmargin})                                              # UPDATE Q-UP MARGIN DICT
            q_upmargin_total += q_upmargin                                                          # INCREMENT TOTAL Q-UP MARGIN
            q_downmargin_dict.update({gidx: q_downmargin})                                          # UPDATE Q-DOWN MARGIN DICT
            q_downmargin_total += q_downmargin                                                      # INCREMENT TOTAL Q-DOWN MARGIN
        if abs(external_pgen) > external_pgen_threshold:                                            # CHECK IF EXTERNAL REAL POWER EXCEED THRESHOLD
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gkey]                                                               # GET THIS PARTICIPATING GENERATOR INDEX
                zeroed = False                                                                      # SET ZEROED FLAG
                pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                # THIS GENERATORS REAL POWER
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
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gkey]                                                               # GET THIS PARTICIPATING GENERATOR INDEX
                zeroed = False                                                                      # SET ZEROED FLAG
                if gidx in swinggen_idxs:                                                           # CHECK IF SWING GENERATOR...
                    continue                                                                        # IF SWING GEN, GET NEXT GENERATOR
                vreg = net_a.res_gen.loc[gidx, 'vm_pu']                                               # THIS GENERATORS VOLTAGE SETPOINT
                if external_qgen < -external_qgen_threshold:                                        # CHECK IF EXTERNAL REACTIVE POWER IS NEGATIVE
                    q_downmargin = q_downmargin_dict[gidx]                                          # GET THIS GENERATORS Q-DOWN MARGIN
                    if vreg < 0.901 or q_downmargin < 1.0:                                          # IF NO MARGIN, OR BUS VOLTAGE IS LOW...
                        continue                                                                        # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total   # CALCULATE SETPOINT INCREMENT (PROPORTIONAL)
                    new_vreg = vreg + delta_vreg                                                        # CALCULATE NEW SET POINT
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                if external_qgen > external_qgen_threshold:                                         # CHECK IF EXTERNAL REACTIVE POWER IS POSITIVE
                    q_upmargin = q_upmargin_dict[gidx]                                              # GET THIS GENERATORS Q-UP MARGIN
                    if vreg > 1.099 or q_upmargin < 1.0:                                            # IF NO MARGIN, OR BUS VOLTAGE IS HIGH...
                        continue                                                                    # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_upmargin_dict[gidx] / q_upmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_vreg = vreg + delta_vreg                                                    # CALCULATE NEW SET POINT
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
        pp.runpp(net_a, enforce_q_lims=True)                                                          # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                  # GET EXTERNAL GRID REAL POWER
        external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                # GET EXTERNAL GRID REACTIVE POWER
        zstep += 1                                                                                   # INCREMENT ITERATOR

    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER
    base_cost = get_generation_cost(net_a, participating_gens, gen_dict, pwlcost_dict0)             # GET TOTAL COST OF GENERATION
    maxloading = get_maxloading(net_a)
    minv, maxv = get_minmax_voltage(net_a)
    print('FINAL SCOPF NETWORK CREATED ........................................', round(time.time() - start_time, 3), 'sec')
    print('GENERATION COST ....................................................', '$ {0:.2f}'.format(base_cost))
    print('MAX BRANCH LOADING .................................................', '{0:.2f} %'.format(maxloading))
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
