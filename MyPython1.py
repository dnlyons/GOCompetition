import os
import sys
import csv
import math
import time
import copy
import pickle
import numpy
import pandapower as pp
from pandas import options as pdoptions
import data as data_read


cwd = os.path.dirname(__file__)
print('PATH TO MYPTHON1 =', cwd)

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
    con_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/case.con'
    inl_fname = cwd + r'/sandbox/Network_01R-10/case.inl'
    raw_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/case.raw'
    rop_fname = cwd + r'/sandbox/Network_01R-10/case.rop'
    outfname1 = cwd + r'/sandbox/Network_01R-10/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/Network_01R-10/scenario_1/solution2.txt'

    neta_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/neta.p'
    netc_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netc.p'
    data_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netdata.pkl'

    try:
        os.remove(outfname1)
        os.remove(outfname2)
    except FileNotFoundError:
        pass


GVREG = 1                   # NON-SWING GENERATORS VOLTAGE SCHEDULES ... 0=DEFAULT_GENV_RAW, 1=CUSTOM(ALL)
SWSHVREG = 1                # SWITCHED SHUNTS VOLTAGE SCHEDULES ........ 0=DEFAULT_RAW, 1=CUSTOM(ALL)
Gvreg_Custom = 1.025
SwShVreg_Custom = 1.025

SwVreg_Init = 1.040         # INITIAL SWING GENERATORS VOLTAGE SETPOINT (THEN CALCULATE FOR VAR MARGIN)


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def listoflists(tt):
    """ convert tuple_of_tuples to list_of_lists """
    return list((listoflists(x) if isinstance(x, tuple) else x for x in tt))


def tupleoftuples(ll):
    """ convert list_of_lists to tuple_of_tuples """
    return tuple((tupleoftuples(x) if isinstance(x, list) else x for x in ll))


def read_data(raw_name, rop_name, inl_name, con_name):
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


def get_swingbus_data(buses):
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus = None
    swangle = 0.0
    for bus in buses.values():
        if bus.ide == 3:
            swbus = bus.i
            swkv = bus.baskv
            swangle = 0.0
            swvhigh = bus.evhi
            swvlow = bus.evlo
            break
    return [swbus, swkv, swangle, swvlow, swvhigh]


def get_swgens_data(swbus, generators):
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


def get_alt_swingbus(generators, buses, swbus, sw_kv, sw_reg):
    # generators = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus1 = None
    sw1gkey = None
    maxp_list = [[g.pt, g.i] for g in generators.values()]
    maxp_list.sort(reverse=True)
    for g in maxp_list:
        gbus = g[1]
        for bus in buses.values():
            if bus.i == gbus and bus.baskv == sw_kv:
                swbus1 = bus.i
                for g in generators.values():
                    if g.i == gbus:
                        g.vs = sw_reg
                        sw1gkey = str(g.i) + '-' + g.id
                        return swbus1, sw1gkey
    return swbus1, sw1gkey


def write_csvdata(fname, lol, label):
    with open(fname, 'a', newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_base_bus_results(fname, b_results, sw_dict, g_results, exgridbus):
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
        mvar_ = 0.0
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / (b_results.loc[bus_, 'voltage_pu'] ** 2)
            buslist[j][3] = mvar_ + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_base_gen_results(fname, g_results, genids, gbuses, swsh_idxs):
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


def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus, clabel):
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
        mvar_ = 0.0
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / (b_results.loc[bus_, 'voltage_pu'] ** 2)
            buslist[j][3] = mvar_ + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_gen_results(fname, g_results, genids, gbuses, delta, swsh_idxs):
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
    # deltapgens = p_delta + pgen_out
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [delta]])
    return


def adj_swshunt_susceptance(_net, _swshidx_dict, _swsh_q_mins, _swsh_q_maxs):
    for _shbus in _swshidx_dict:                                                    # loop across swshunt (gen) buses
        _shidx = _swshidx_dict[_shbus]                                              # get swshunt index
        _busv = _net.res_bus.loc[_shbus, 'vm_pu']                                   # get swshunt bus voltage
        _maxq = _swsh_q_maxs[_shidx]
        _mvar = _net.res_gen.loc[_shidx, 'q_mvar']                                  # get swshunt vars
        if _busv > 1.0:                                                                              # IF BUS VOLTAGE > 1.0 PU...
            _next_mvar = _maxq * _busv ** 2
            _net.gen.loc[_shidx, 'max_q_mvar'] = _next_mvar                                          # SET MIN SWSHUNT VARS
        elif _busv < 1.0:                                                                            # IF BUS VOLTAGE < 1.0 PU...
            _next_mvar = _maxq * _busv ** 2
            _net.gen.loc[_shidx, 'min_q_mvar'] = _next_mvar                                          # SET MAX SWSHUNT VARS
        # print(_busv > 1.0, _shbus, _busv, _mvar, _next_mvar, _maxq)
    return _net


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


def get_generation_cost(_net, _gen_dict, _pwlcost_dict0):
    _cost = 0.0
    for _gkey in _pwlcost_dict0:                                                                   # LOOP ACROSS PARTICIPATING GENERATORS
        _gidx = _gen_dict[_gkey]
        _pcostdata = _pwlcost_dict0[_gkey]
        _g_mw = _net.res_gen.loc[_gidx, 'p_mw']
        _xlist, _ylist = zip(*_pcostdata)
        _cost += numpy.interp(_g_mw, _xlist, _ylist)
    return _cost


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 100
    pdoptions.display.max_rows = 1000
    pdoptions.display.max_colwidth = 100
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


# =================================================================================================
# -- MYPYTHON_1 -----------------------------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    print()
    start_time = time.time()

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
    swingbus, swing_kv, swing_angle, swing_vlow, swing_vhigh = get_swingbus_data(raw_data.raw.buses)

    # -- GET SWING GEN DATA FROM GENDATA (REMOVE SWING GEN FROM GENDATA) --------------------------
    swgens_data = get_swgens_data(swingbus, raw_data.raw.generators)

    # -- GET ALTERNATE SWING BUS ------------------------------------------------------------------
    swingbus1, sw1_genkey = get_alt_swingbus(raw_data.raw.generators, raw_data.raw.buses, swingbus, swing_kv, swgens_data[0][6])

    # =============================================================================================
    # == CREATE NETWORK ===========================================================================
    # =============================================================================================
    print('------------------------ CREATING NETWORKS -------------------------')
    create_starttime = time.time()
    net_a = pp.create_empty_network('net_a', 60.0, mva_base)
    net_c = pp.create_empty_network('net_c', 60.0, mva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    busnomkv_dict = {}
    buskv_dict = {}
    busarea_dict = {}
    busidxs = []
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
        pp.create_bus(net_a, vn_kv=busnomkv, zone=busarea, max_vm_pu=bus.nvhi, min_vm_pu=bus.nvlo, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        idx = pp.create_bus(net_c, vn_kv=busnomkv, zone=busarea, max_vm_pu=bus.evhi, min_vm_pu=bus.evlo, index=busnum)
        if busnum == swingbus:
            swingbus_idx = idx
        busnomkv_dict.update({busnum: busnomkv})
        buskv_dict.update({busnum: buskv})
        busarea_dict.update({busnum: busarea})
        busidxs.append(idx)

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
        pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)
        pp.create_load(net_c, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    gids = []
    gen_dict = {}
    genidx_dict = {}
    swinggen_idxs = []
    gen_status_vreg_dict = {}
    genarea_dict = {}
    genidxs = []
    genbus_dict = {}
    participating_gens = []
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
        vreg = SwVreg_Init
        status = swgen_data[9]
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        power_factor = pmax / genmva
        gen_status_vreg_dict.update({genbus: [True, vreg]})
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25,  cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status, index=idx)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
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
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        gids.append("'" + gid + "'")
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genbus_dict.update({genkey: genbus})

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
        gen_status_vreg_dict.update({genbus: [False, vreg]})
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
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status, index=idx)
        if status:
            gen_status_vreg_dict[genbus][0] = status
        gids.append("'" + gid + "'")
        gen_dict.update({genkey: idx})
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genbus_dict.update({genkey: genbus})

    # == ADD FIXED SHUNT DATA TO NETWORK ==========================================================
    # fixshunt = (fxshunt.i, fxshunt.id, fxshunt.status, fxshunt.gl, fxshunt.bl)
    fxidxdict = {}
    if raw_data.raw.fixed_shunts.values():
        print('ADD FIXED SHUNTS ...................................................')
        for fxshunt in raw_data.raw.fixed_shunts.values():
            status = bool(fxshunt.status)
            if not status:
                continue
            shuntbus = fxshunt.i
            shuntname = str(shuntbus) + '-FX'
            mw = fxshunt.gl
            mvar = fxshunt.bl
            nomkv = busnomkv_dict[shuntbus]

            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_shunt(net_a, shuntbus, vn_kv=nomkv, q_mvar=fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxidxdict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
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
            if shuntbus in gen_status_vreg_dict:
                if gen_status_vreg_dict[shuntbus][0]:
                    vreg = gen_status_vreg_dict[shuntbus][1]
            steps = [swshunt.n1, swshunt.n2, swshunt.n3, swshunt.n4, swshunt.n5, swshunt.n6, swshunt.n7, swshunt.n8]
            kvars = [swshunt.b1, swshunt.b2, swshunt.b3, swshunt.b4, swshunt.b5, swshunt.b6, swshunt.b7, swshunt.b8]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(kvars)):
                if kvars[j] < 0.0:
                    total_qmin += steps[j] * kvars[j]
                elif kvars[j] > 0.0:
                    total_qmax += steps[j] * kvars[j]
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

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st)
    line_dict = {}
    line_ratea_dict = {}
    lineidxs = []
    print('ADD LINES ..........................................................')
    for line in raw_data.raw.nontransformer_branches.values():
        frombus = line.i
        tobus = line.j
        ckt = line.ckt
        status = bool(line.st)
        length = 1.0
        kv = busnomkv_dict[frombus]
        zbase = kv ** 2 / mva_base
        r_pu = line.r
        x_pu = line.x
        b_pu = line.b
        r = r_pu * zbase
        x = x_pu * zbase
        b = b_pu / zbase
        capacitance = 1e9 * b / (2 * math.pi * 60.0)
        base_mva_rating = line.ratea
        mva_rating = line.ratec
        i_rating_a = base_mva_rating / (math.sqrt(3) * kv)
        i_rating_c = mva_rating / (math.sqrt(3) * kv)
        linekey = str(frombus) + '-' + str(tobus) + '-' + ckt
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey, max_loading_percent=100.0, in_service=status)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey, max_loading_percent=100.0, in_service=status, index=idx)
        line_dict.update({linekey: idx})
        lineidxs.append(idx)

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.stat)
    xfmr_dict = {}
    xfmr_ratea_dict = {}
    xfmridxs = []
    print('ADD 2W TRANSFORMERS ................................................')
    for xfmr in raw_data.raw.transformers.values():
        status = bool(xfmr.stat)
        frombus = xfmr.i
        tobus = xfmr.j
        ckt = xfmr.ckt
        xfmrkey = str(frombus) + '-' + str(tobus) + '-' + ckt
        fromkv = busnomkv_dict[frombus]
        tokv = busnomkv_dict[tobus]
        if fromkv < tokv:                           # force from bus to be highside
            frombus, tobus = tobus, frombus
            fromkv, tokv = tokv, fromkv
        r_pu_sbase = xfmr.r12                             # @ mva_base
        x_pu_sbase = xfmr.x12                             # @ mva_base
        r_pu = r_pu_sbase * xfmr.rata1 / mva_base   # pandapower uses given transformer rating as test mva
        x_pu = x_pu_sbase * xfmr.rata1 / mva_base   # so convert to mva_rating base
        z_pu = math.sqrt(r_pu ** 2 + x_pu ** 2)  # calculate 'nameplate' pu impedance
        z_pct = 100.0 * z_pu                      # pandadower uses percent impedance
        r_pct = 100.0 * r_pu                      # pandadower uses percent resistance
        noloadlosses = 0.0
        ironlosses = 0.0
        # -- TAP SETTINGS -----------------------------------------------------
        shiftdegree = 0.0
        tapside = 'hv'
        tappos = 3
        tapnuetral = 3
        tapmax = 5
        tapmin = 1
        tapsteppercent = 2.5
        tapstepdegree = 0.0
        tapphaseshifter = False
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_transformer_from_parameters(net_a, frombus, tobus, xfmr.rata1, fromkv, tokv, r_pct, z_pct, ironlosses, noloadlosses,
                                                    shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapnuetral, tap_max=tapmax, tap_min=tapmin,
                                                    tap_step_percent=tapsteppercent, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                                    in_service=status, name=xfmrkey, max_loading_percent=100.0, parallel=1, df=1.0)

        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_transformer_from_parameters(net_c, frombus, tobus, xfmr.rata1, fromkv, tokv, r_pct, z_pct, ironlosses, noloadlosses,
                                              shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapnuetral, tap_max=tapmax, tap_min=tapmin,
                                              tap_step_percent=tapsteppercent, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                              in_service=status, name=xfmrkey, max_loading_percent=100.0, parallel=1, df=1.0, index=idx)
        xfmr_dict.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmridxs.append(idx)

    # == ADD EXTERNAL GRID ========================================================================
    # == WITH DUMMY TIE TO RAW SWING BUS ==========================================================
    # == DUMMY TIE RATING UP WITH LOWER KV ========================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)

    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.011, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, type='p')
    # pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=1, cq0_eur=1e6, type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
    tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.01, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, cq1_eur_per_mvar=0, cq0_eur=1e9)
    # pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=0, cq0_eur=1e9, type='q')

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 3))

    # == MISC NETWORK DATA ========================================================================
    # swsh_q_mins = {}
    # swsh_q_maxs = {}
    # for swshidx in swshidxs:
    #     swsh_q_min = net_a.gen.loc[swshidx, 'min_q_mvar']
    #     swsh_q_max = net_a.gen.loc[swshidx, 'max_q_mvar']
    #     swsh_q_mins.update({swshidx: swsh_q_min})
    #     swsh_q_maxs.update({swshidx: swsh_q_max})
    #
    goutagekeys = outage_dict['gen'].keys()                                                         # GET OUTAGED GENERATOR KEYS
    boutagekeys = outage_dict['branch'].keys()                                                      # GET OUTAGED BRANCH KEYS

    online_gens = []                                                                                # INITIALIZE ONLINE GENERATOR LIST
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        if not net_a.gen.loc[gidx, 'in_service']:                                                   # CHECK IF GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT GENERATOR KEY
        online_gens.append(gkey)                                                                    # ADD ONLINE GENERATOR TO LIST

    external_pgen_threshold = 1e-4
    external_qgen_threshold = 1e-4
    swing_qmin = net_a.gen.loc[swinggen_idxs[0], 'min_q_mvar']
    swing_qmax = net_a.gen.loc[swinggen_idxs[0], 'max_q_mvar']

    levels_out = 3                                                                                  # DEFINE NLEVELS
    nlevel_buses = [swingbus]                                                                       # INITIALIZE BUS LIST
    for i in range(levels_out):                                                                     # LOOP THROUGH HOW MANY LEVELS...
        nlevel_buses = pp.get_connected_buses(net_a, nlevel_buses, respect_in_service=True)         # NEW BUSES ARE ADDED TO BUSLIST
    nlevel_buses = [x for x in nlevel_buses if x != ext_grid_idx]                                   # REMOVE EXTGRID BUS FROM BUSLIST
    # =============================================================================================

    # -- SOLVE INITIAL NETWORKS WITH STRAIGHT POWERFLOW -------------------------------------------
    solve_starttime = time.time()
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK
    print('   NETWORKS SOLVED .................................................', round(time.time() - solve_starttime, 3))

    # ---------------------------------------------------------------------------------------------
    # -- CREATE INITIAL BASECASE OPF --------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    print('------------------ CREATING INITIAL BASECASE OPF -------------------')
    opf_startime = time.time()
    pp.runopp(net_a, enforce_q_lims=True)                                                           # RUN OPF ON THIS NETWORK
    net_c.gen['p_mw'] = net_a.res_gen['p_mw']                                                       # SET THIS NETWORK GENERATORS POWER TO OPF GENERATORS POWER (WITH OUTAGE)
    for genbus in genidx_dict:                                                                      # LOOP ACROSS GENERATOR BUSES
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        gidx = genidx_dict[genbus]                                                                  # GET GENERATOR INDEX
        net_a.gen.loc[gidx, 'vm_pu'] = net_a.res_bus.loc[genbus, 'vm_pu']                           # SET THIS NETWORK GENS VREG TO OPF RESULTS
        net_c.gen.loc[gidx, 'vm_pu'] = net_a.res_bus.loc[genbus, 'vm_pu']                           # SET THIS NETWORK GENS VREG TO OPF RESULTS
    for shbus in swshidx_dict:                                                                      # LOOP ACROSS SWSHUNT (GEN) BUSES
        shidx = swshidx_dict[shbus]                                                                 # GET SWSHUNT INDEX
        net_a.gen.loc[shidx, 'vm_pu'] = net_a.res_bus.loc[shbus, 'vm_pu']                           # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
        net_c.gen.loc[shidx, 'vm_pu'] = net_a.res_bus.loc[shbus, 'vm_pu']                           # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
    nlevel_buses_v = [x for x in net_a.res_bus.loc[nlevel_buses, 'vm_pu']]                          # GET LIST OF NLEVEL BUS VOLTAGES
    max_v = max(nlevel_buses_v)
    net_a.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    init_cost = get_generation_cost(net_a, gen_dict, pwlcost_dict0)
    print('INITIAL BASECASE OPF CREATED .............. ${0:8.1f} ..............'.format(init_cost), round(time.time() - opf_startime, 3))
    print('INITIAL SWING VREG SETPOINT = {0:.4f} ...............................'.format(max_v))

    # ---------------------------------------------------------------------------------------------
    # -- CHECK FOR SOLVED AND NOT SOLVED OUTAGES WITH STRAIGHT POWER FLOW -------------------------
    # ---------------------------------------------------------------------------------------------
    print('---------------- CHECKING FOR NOSOLVE CONTINGENCIES ----------------')
    ns_net = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    solved_outages = []                                                                             # INITIALIZE SOLVED OUTAGES LIST
    nosolve_outages0 = []                                                                           # INITIALIZE NOSOLVE OUTAGES LIST
    # -- CHECK CONTINGENCIES FOR NO SOLUTION ----------------------------------
    ns_text = ''                                                                                    # INITIALIZE OUTAGE TEXT
    for gkey in goutagekeys:                                                                        # LOOP THROUGH GENERATOR OUTAGES
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        ns_text = 'GEN '                                                                            # ASSIGN TEXT
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        net.gen.in_service[gidx] = False                                                            # SWITCH OFF OUTAGED GENERATOR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
            solved_outages.append(gkey)                                                             # APPEND SOLVED OUTAGES LIST
        except:                                                                                     # IF NO SOLUTION...
            nosolve_outages0.append(gkey)                                                                   # APPEND NOT SOLVED OUTAGES LIST
            print(ns_text, '{0:9s} Not Solved with Initial OPF Basecase ................'.format(gkey))     # PRINT INFO ON OUTAGE
    for bkey in boutagekeys:                                                                                # LOOP THROUGH BRANCH OUTAGES
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        if bkey in line_dict:                                                                       # CHECK IF BRANCH IS A LINE...
            ns_text = 'LINE'                                                                        # ASSIGN TEXT
            lineidx = line_dict[bkey]                                                               # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif bkey in xfmr_dict:                                                                     # CHECK IF BRANCH IS A XFMR...
            ns_text = 'XFMR'                                                                        # ASSIGN TEXT
            xfmridx = xfmr_dict[bkey]                                                               # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # RUN POWERFLOW
            solved_outages.append(bkey)                                                             # APPEND NO SOLVE OUTAGES LIST
        except:                                                                                     # IF NO SOLUTION...
            nosolve_outages0.append(bkey)                                                                   # APPEND NOT SOLVED OUTAGES LIST
            print(ns_text, '{0:9s} Not Solved with Initial OPF Basecase ................'.format(bkey))     # PRINT INFO ON OUTAGE

    # ---------------------------------------------------------------------------------------------
    # -- FIND OUTAGES RESULTING IN LOADING CONSTRAINTS --------------------------------------------
    # -- THEN ITERATE WITH OPF TO FINE OPTIMAL BASECASE OPERATING POINT ---------------------------
    # ---------------------------------------------------------------------------------------------
    print('------------- FINDING OPTIMAL BASECASE OPERATING POINT -------------')
    scopf_startime = time.time()
    # -- FIND SOLVED OUTAGES WITH LOADING CONSTRAINTS ---------------------------------------------
    scopf_net = copy.deepcopy(net_c)                                                                # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    constrained_outages = []                                                                        # INITIALIZE CONSTRAINED OUTAGES LIST
    constraint = False                                                                              # INITIALIZE CONTRAINT FLAG
    scopf_text = ''                                                                                 # INITIALIZE OUTAGE TEXT
    for scopf_key in solved_outages:                                                                # LOOP ACROSS SOLVED CONTINGENCIES
        net = copy.deepcopy(scopf_net)                                                              # GET FRESH COPY OF NOSOLVE NETWORK
        if scopf_key in online_gens:                                                                # CHECK IF A GENERATOR...
            scopf_text = 'GEN '                                                                     # ASSIGN TEXT
            gidx = gen_dict[scopf_key]                                                              # GET GENERATOR INDEX
            net.gen.in_service[gidx] = False                                                        # SWITCH OFF OUTAGED GENERATOR
        elif scopf_key in line_dict:                                                                # CHECK IF A LINE...
            scopf_text = 'LINE'                                                                     # ASSIGN TEXT
            lineidx = line_dict[scopf_key]                                                          # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif scopf_key in xfmr_dict:                                                                # CHECK IF A XFMR...
            scopf_text = 'XFMR'                                                                     # ASSIGN TEXT
            xfmridx = xfmr_dict[scopf_key]                                                          # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
            line_loading = max(net.res_line['loading_percent'].values)                              # GET MAX LINE LOADING
            xfmr_loading = max(net.res_trafo['loading_percent'].values)                             # GET MAX LINE LOADING
            max_loading = max(line_loading, xfmr_loading)                                           # GET MAX OF MAXs
            if max_loading > 100.0:                                                                 # IF MAX LOADING < 100%
                constraint = True                                                                   # SET CONSTRAINT FLAG
                constrained_outages.append([max_loading, scopf_key])                                # ADD OUTAGE TO CONSTRAINED OUTAGES LIST
        except:                                                                                                     # IF NO SOLUTION...
            print(scopf_text, '{0:9s} NOT SOLVED USING INITIAL OPF BASECASE ...............'.format(scopf_key))     # PRINT INFO ON OUTAGE
    constrained_outages.sort(reverse=True)                                                                          # SORT CONSTRAINED OUTAGES LIST
    # constrained_outages.sort()                                                                    # SORT CONSTRAINED OUTAGES LIST
    constrained_outages = [x[1] for x in constrained_outages]                                       # GET ONLY KEYS OF CONTRAINED OUTAGES

    # -- ITERATE WITH OPF TO MINIMIZE CONSTRAINED OUTAGES -----------------------------------------
    step = 1                                                                                        # INITIALIZE ITERATOR
    scopf_net = copy.deepcopy(net_c)                                                                # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    while constraint and step < 2:                                                                  # LOOP WHILE CONSTRAINT EXISTS
        contraint = False                                                                           # SET CONSTRAINT FLAG
        for scopf_key in constrained_outages:                                                       # LOOP THROUGH CONSTRAINED OUTAGES
            constrained_outages1 = []                                                               # INITIALIZE A 'SUB' CONTRAINED OUTAGES LIST
            net = copy.deepcopy(scopf_net)                                                          # GET FRESH COPY OF SCOPF NETWORK
            if scopf_key in online_gens:                                                            # CHECK IF A GENERATOR...
                scopf_text = 'GEN '                                                                 # ASSIGN TEXT
                gidx = gen_dict[scopf_key]                                                          # GET GENERATOR INDEX
                net.gen.in_service[gidx] = False                                                    # SWITCH OFF OUTAGED GENERATOR
            elif scopf_key in line_dict:                                                            # CHECK IF A LINE...
                scopf_text = 'LINE'                                                                 # ASSIGN TEXT
                lineidx = line_dict[scopf_key]                                                      # GET LINE INDEX
                net.line.in_service[lineidx] = False                                                # SWITCH OUT OUTAGED LINE
            elif scopf_key in xfmr_dict:                                                            # CHECK IF A XFMR...
                scopf_text = 'XFMR'                                                                 # ASSIGN TEXT
                xfmridx = xfmr_dict[scopf_key]                                                      # GET XFMR INDEX
                net.trafo.in_service[xfmridx] = False                                               # SWITCH OUT OUTAGED XFMR
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW ON OUTAGE
            line_loading = max(net.res_line['loading_percent'].values)                              # GET MAX LINE LOADING
            xfmr_loading = max(net.res_trafo['loading_percent'].values)                             # GET MAX LINE LOADING
            max_loading = max(line_loading, xfmr_loading)                                           # GET MAX OF MAXs
            if max_loading > 100.0:                                                                                                 # IF MAX LOADING < 100%
                print(scopf_text, '{0:9s} Outage Results in Loading Constraint ({1:.1f}%) .......'.format(scopf_key, max_loading))  # PRINT INFO ON OUTAGE
                constraint = True                                                                                                   # SET CONSTRAINT FLAG
                constrained_outages1.append(scopf_key)                                              # ADD OUTAGE TO 'SUB CONSTRAINED OUTAGES LIST
                pp.runopp(net, enforce_q_lims=True)                                                 # RUN OPF ON THIS NETWORK
                scopf_net.gen['p_mw'] = net.res_gen['p_mw']                                         # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                net_a.gen['p_mw'] = net.res_gen['p_mw']                                             # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                net_c.gen['p_mw'] = net.res_gen['p_mw']                                             # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                for genbus in genidx_dict:                                                          # LOOP ACROSS GENERATOR BUSES
                    if genbus == swingbus:                                                          # CHECK IF SWING SWING BUS...
                        continue                                                                    # IF SWING BUS, GET NEXT GENERATOR
                    gidx = genidx_dict[genbus]                                                      # GET GENERATOR INDEX
                    scopf_net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']             # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                    net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                 # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                    net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                 # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                for shbus in swshidx_dict:                                                          # LOOP ACROSS SWSHUNT (GEN) BUSES
                    shidx = swshidx_dict[shbus]                                                     # GET SWSHUNT INDEX
                    scopf_net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']             # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
                    net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                 # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
                    net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                 # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
            pp.runpp(scopf_net, enforce_q_lims=True)                                                # RUN STRAIGHT POWER FLOW ON SCOPF NETWORK
            # pp.runpp(net_a, enforce_q_lims=True)                                                    # RUN STRAIGHT POWER FLOW ON BASECASE NETWORK
            # pp.runpp(net_c, enforce_q_lims=True)                                                    # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE NETWORK
        constrained_outages = constrained_outages1                                                  # REDUCE NEXT ITERATION CONSTRAINED OUTAGES
        step += 1                                                                                   # INCREMENT ITERATOR
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID REAL POWER
    external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                  # GET EXTERNAL GRID REACTIVE POWER
    print('--------------------------------------------------------------------')

    # ---------------------------------------------------------------------------------------------
    # -- INSURE MVAR MARGIN ON SWING GENERATORS ---------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    net = copy.deepcopy(net_a)                                                                      # GET FRESH COPY OF BASECASE NETWORK
    sw_qmin = 0.90 * swing_qmin                                                                     # DEFINE SWING GENERATORS QMIN FOR 10% MARGIN
    sw_qmax = 0.90 * swing_qmin                                                                     # DEFINE SWING GENERATORS QMAX FOR 10% MARGIN
    swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                           # GET SWING GENERATORS REACTIVE POWER
    sw_vreg = net.res_gen.loc[swinggen_idxs[0], 'vm_pu']                                            # GET SWING GENERATORS VOLTAGE SETPOINT

    if swing_q < sw_qmin:                                                                           # CHECK IF SWING MVAR IS LESS THAN THRESHOLD
        step = 1                                                                                    # INITIALIZE ITERATOR
        while swing_q < sw_qmin and step < 40:                                                      # LOOP WHILE SWING MVAR IS LESS THAN THRESHOLD
            sw_vreg += 0.0005                                                                       # INCREASE SWING GENERATORS VOLTAGE SETPOINT
            sw_vreg = min(sw_vreg, sw_vmax_a - 0.0001)                                              # INSURE SETPOINT IS LESS THAN MAX BUS VOLTAGE
            net.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                           # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                       # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_a.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_c.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            pp.runpp(net, enforce_q_lims=True)                                                      # THIS NETWORK, RUN STRAIGHT POWER
            swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                   # GET SWING GENERATORS NEW REACTIVE POWER
            step += 1                                                                               # INCREMENT ITERATOR
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON BASECASE
        pp.runpp(net_c, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE

    elif swing_q > sw_qmax:                                                                         # CHECK IF SWING MVAR IS MORE THAN THRESHOLD
        step = 1                                                                                    # INITIALIZE ITERATOR
        while swing_q > sw_qmax and step < 40:                                                      # LOOP WHILE SWING MVAR IS MORE THAN THRESHOLD
            sw_vreg -= 0.0005                                                                       # DECREASE SWING GENERATORS VOLTAGE SETPOINT
            sw_vreg = min(sw_vreg, sw_vmin_a + 0.0001)                                              # INSURE SETPOINT IS MORE THAN MIN BUS VOLTAGE
            net.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                           # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                       # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_a.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_c.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            pp.runpp(net, enforce_q_lims=True)                                                      # THIS NETWORK, RUN STRAIGHT POWER
            swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                   # GET SWING GENERATORS NEW REACTIVE POWER
            step += 1                                                                               # INCREMENT ITERATOR
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON BASECASE
        pp.runpp(net_c, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE

    # ---------------------------------------------------------------------------------------------
    # -- ZERO OUT EXTERNAL GRID REAL AND REACTIVE POWER CONTRIBUTIONS -----------------------------
    # ---------------------------------------------------------------------------------------------
    net = copy.deepcopy(net_a)
    zeroed = abs(external_pgen) < external_pgen_threshold or abs(external_qgen) < external_qgen_threshold
    step = 1
    while not zeroed and step < 120:
        zeroed = True
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
            pgen = net.res_gen.loc[gidx, 'p_mw']                                                    # THIS GENERATORS PGEN
            pmin = net.gen.loc[gidx, 'min_p_mw']                                                    # THIS GENERATORS PMIN
            pmax = net.gen.loc[gidx, 'max_p_mw']                                                    # THIS GENERATORS PMAX
            p_upmargin = pmax - pgen                                                                # THIS GENERATORS P-UP MARGIN
            p_downmargin = pgen - pmin                                                              # THIS GENERATORS P-DOWN MARGIN
            p_upmargin_dict.update({gidx: p_upmargin})                                              # UPDATE P-UP MARGIN DICT
            p_upmargin_total += p_upmargin                                                          # INCREMENT TOTAL P-UP MARGIN
            p_downmargin_dict.update({gidx: p_downmargin})                                          # UPDATE P-DOWN MARGIN DICT
            p_downmargin_total += p_downmargin                                                      # INCREMENT TOTAL P-DOWN MARGIN
            qgen = net.res_gen.loc[gidx, 'q_mvar']                                                  # THIS GENERATORS QGEN
            qmin = net.gen.loc[gidx, 'min_q_mvar']                                                  # THIS GENERATORS QMIN
            qmax = net.gen.loc[gidx, 'max_q_mvar']                                                  # THIS GENERATORS QMAX
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
                pgen = net.res_gen.loc[gidx, 'p_mw']                                                # THIS GENERATORS REAL POWER
                if external_pgen < -external_pgen_threshold:                                        # CHECK IF EXTERNAL REAL POWER IS NEGATIVE
                    p_downmargin = p_downmargin_dict[gidx]                                          # GET THIS GENERATORS P-DOWN MARGIN
                    if p_downmargin < 1.0:                                                          # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_downmargin_dict[gidx] / p_downmargin_total       # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net.gen.loc[gidx, 'p_mw'] = new_pgen                                            # SET GENERATOR PGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                if external_pgen > external_pgen_threshold:                                         # CHECK IF EXTERNAL REAL POWER IS POSITIVE
                    p_upmargin = p_upmargin_dict[gidx]                                              # GET THIS GENERATORS P-UP MARGIN
                    if p_upmargin < 1.0:                                                            # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_upmargin / p_upmargin_total                      # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net.gen.loc[gidx, 'p_mw'] = new_pgen                                            # SET GENERATOR PGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
        if abs(external_qgen) > external_qgen_threshold:                                            # CHECK IF EXTERNAL REACTIVE POWER EXCEED THRESHOLD
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gkey]                                                               # GET THIS PARTICIPATING GENERATOR INDEX
                zeroed = False                                                                      # SET ZEROED FLAG
                if gidx in swinggen_idxs:                                                           # CHECK IF SWING GENERATOR...
                    continue                                                                        # IF SWING GEN, GET NEXT GENERATOR
                vreg = net.res_gen.loc[gidx, 'vm_pu']                                               # THIS GENERATORS VOLTAGE SETPOINT
                if external_qgen < -external_qgen_threshold:                                        # CHECK IF EXTERNAL REACTIVE POWER IS NEGATIVE
                    q_downmargin = q_downmargin_dict[gidx]                                          # GET THIS GENERATORS Q-DOWN MARGIN
                    if vreg < 0.901 or q_downmargin < 1.0:                                          # IF NO MARGIN, OR BUS VOLTAGE IS LOW...
                        continue                                                                        # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_vreg = vreg + delta_vreg                                                        # CALCULATE NEW SET POINT
                    net.gen.loc[gidx, 'vm_pu'] = new_vreg                                           # SET GENERATOR QGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                if external_qgen > external_qgen_threshold:                                         # CHECK IF EXTERNAL REACTIVE POWER IS POSITIVE
                    q_upmargin = q_upmargin_dict[gidx]                                              # GET THIS GENERATORS Q-UP MARGIN
                    if vreg > 1.099 or q_upmargin < 1.0:                                            # IF NO MARGIN, OR BUS VOLTAGE IS HIGH...
                        continue                                                                    # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_upmargin_dict[gidx] / q_upmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_vreg = vreg + delta_vreg                                                    # CALCULATE NEW SET POINT
                    net.gen.loc[gidx, 'vm_pu'] = new_vreg                                           # SET GENERATOR QGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
        pp.runpp(net, enforce_q_lims=True)                                                          # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        external_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                  # GET EXTERNAL GRID REAL POWER
        external_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                # GET EXTERNAL GRID REACTIVE POWER
        step += 1                                                                                   # INCREMENT ITERATOR
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    base_cost = get_generation_cost(net_a, gen_dict, pwlcost_dict0)                                 # GET TOTAL COST OF GENERATION
    print('BASECASE OPF CREATED .................. ${0:8.1f} ..................'.format(base_cost), round(time.time() - scopf_startime, 3))
    print('SWING VREG SETPOINT = {0:.4f} .......................................'.format(sw_vreg))
    # ---------------------------------------------------------------------------------------------
    # -- CURIOSITY CHECK IF NOSOLVE OUTAGES NOW SOLVE WITH THIS BASECASE OPF ----------------------
    # ---------------------------------------------------------------------------------------------
    ns_net = copy.deepcopy(net_a)                                                                   # GET FRESH COPY OF BASECASE NETWORK
    ns_text = ''                                                                                    # INITIALIZE OUTAGE TEXT
    for nskey in nosolve_outages0:                                                                  # LOOP ACROSS SOLVED KEYS
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        if nskey in online_gens:                                                                    # CHECK IF A GENERATOR...
            ns_text = 'GEN '                                                                        # ASSIGN TEXT
            gidx = gen_dict[nskey]                                                                  # GET GENERATOR INDEX
            net.gen.in_service[gidx] = False                                                        # SWITCH OFF OUTAGED GENERATOR
        elif nskey in line_dict:                                                                    # CHECK IF A LINE...
            ns_text = 'LINE'                                                                        # ASSIGN TEXT
            lineidx = line_dict[nskey]                                                              # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif nskey in xfmr_dict:                                                                    # CHECK IF A XFMR...
            ns_text = 'XFMR'                                                                        # ASSIGN TEXT
            xfmridx = xfmr_dict[nskey]                                                              # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
        except:                                                                                             # IF NO SOLUTION...
            print(ns_text, '{0:9s} STILL NOT SOLVED USING BASECASE OPF .................'.format(nskey))    # PRINT INFO ON OUTAGE

    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER

    # -- GET BASECASE GENERATORS POWER OUTPUT -----------------------------------------------------
    base_pgens = {}                                                                                 # INITIALIZE BASECASE GENERATOR POWER DICT
    for gkey in online_gens:                                                                        # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        base_pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                 # GET THIS GENERATOR POWER OUTPUT
        base_pgens.update({gkey: base_pgen})                                                        # UPDATE BASECASE GENERATOR POWER DICT

    # ---------------------------------------------------------------------------------------------
    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    # ---------------------------------------------------------------------------------------------
    print('WRITING BASECASE RESULTS TO FILE ..... {0:.5f} MW {1:.5f} MVAR .....'.format(ex_pgen, ex_qgen))
    bus_results = copy.deepcopy(net_a.res_bus)                                                      # GET BASECASE BUS RESULTS
    gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
    write_base_bus_results(outfname1, bus_results, swshidx_dict, gen_results, ext_grid_idx)         # WRITE SOLUTION1 BUS RESULTS
    write_base_gen_results(outfname1, gen_results, gids, genbuses, swshidxs)                        # WRITE SOLUTION1 GEN RESULTS

    # ---------------------------------------------------------------------------------------------
    # == WRITE DATA TO FILE =======================================================================
    # ---------------------------------------------------------------------------------------------
    print('====================================================================')
    write_starttime = time.time()
    # -- WRITE RATEA NETWORK TO FILE ----------------------
    pp.to_pickle(net_a, neta_fname)
    # -- WRITE RATEC NETWORK TO FILE ----------------------
    pp.to_pickle(net_c, netc_fname)
    # -- WRITE DATA TO FILE -------------------------------
    PFile = open(data_fname, 'wb')
    pickle.dump([outage_dict, gen_dict, xfmr_dict, pfactor_dict, ext_grid_idx, gids, genbuses, swshidxs, swshidx_dict,
                 line_dict, genidx_dict, swinggen_idxs, online_gens, base_pgens, participating_gens], PFile)
    PFile.close()
    print('WRITING DATA TO FILE -----------------------------------------------', round(time.time() - write_starttime, 3))
    print('DONE ---------------------------------------------------------------')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))



    print()
    print()
    print()
    print('TESTING CALCULATING DELTA VARIABLES --------------------------------')
    # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- RUN GENERATOR OUTAGES TO FIND OPTIMUM DELTA VARIABLE -------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    gdelta_starttime = time.time()
    # cnet = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY NETWORK
    # for genbus in genidx_dict:                                                                    # LOOP ACROSS GENERATOR BUSES
    #     gidx = genidx_dict[genbus]                                                                # GET GENERATOR INDEX
    #     cnet.gen.loc[gidx, 'controllable'] = False                                                # TURN OFF GENERATOR OPF CONTROL (NOT SWSHUNTS)
    #
    # # -- TODO DOES OPTIMIZE SHUNTS GO BEFORE LAST DELTA ITERATION?
    # pp.runopp(cnet, enforce_q_lims=True)                                                          # RUN OPTIMAL POWER FLOW (OPTIMIZE SWSHUNTS)
    # for shbus in swshidx_dict:                                                                    # LOOP ACROSS SWSHUNT (GEN) BUSES
    #     shidx = swshidx_dict[shbus]                                                               # GET SWSHUNT INDEX
    #     cnet.gen.loc[shidx, 'vm_pu'] = cnet.res_bus.loc[shbus, 'vm_pu']                           # SET SWSHUNT VREG TO OPF RESULTS
    # pp.runpp(cnet, enforce_q_lims=True)                                                           # RUN STRAIGHT POWER FLOW

    for genkey in goutagekeys:                                                                      # LOOP THROUGH GENERATOR OUTAGES
        gnet = copy.deepcopy(net_c)                                                                 # GET FRESH COPY OF CONTINGENCY NETWORK
        if genkey not in online_gens:
            print('genkey not in online gens')
            continue                                                                                # IF OFFLINE, GET NEXT GENERATOR
        genidx = gen_dict[genkey]                                                                   # GET OUTAGED GENERATOR INDEX
        conlabel = "'" + outage_dict['gen'][genkey] + "'"                                           # GET CONTINGENCY LABEL
        gen_outage_p = base_pgens[genkey]                                                           # GET OUTAGED GENERATOR PGEN
        gnet.gen.in_service[genidx] = False                                                         # SWITCH OFF OUTAGED GENERATOR

        # -- FIRST ESTIMATE OF DELTA VARIABLE -----------------------------------------------------
        pfactor_total = 0.0                                                                         # INITIALIZE PARTICIPATION FACTORS TOTAL
        for gen_pkey in participating_gens:                                                         # LOOP ACROSS PARTICIPATING GENERATORS
            if gen_pkey == genkey:                                                                  # DON'T WANT PFACTOR FOR OUTAGED GENERATOR
                continue                                                                            # GET NEXT PARTICIPATING GENERATOR
            gidx = gen_dict[gen_pkey]                                                               # GET PARTICIPATING GENERATOR INDEX
            pfactor_total += pfactor_dict[gen_pkey]                                                 # INCREMENT PFACTOR TOTAL
        pp.runpp(gnet, enforce_q_lims=True)                                                         # RUN STRAIGHT POWER FLOW
        ex_pgen = gnet.res_ext_grid.loc[ext_grid_idx, 'p_mw']
        delta_init = (gen_outage_p + ex_pgen) / pfactor_total                                      # INITIAL DELTA VARIABLE ESTIMATE
        # delta_init = ex_pgen / (pfactor_total * (1 - math.exp(-1)))                              # INITIAL DELTA VARIABLE ESTIMATE
        # delta_init = (gen_outage_p + ex_pgen) / (pfactor_total * (1 - math.exp(-1)))             # INITIAL DELTA VARIABLE ESTIMATE

        # if genidx in swinggen_idxs:
        #     external_qgen = gnet.res_ext_grid.loc[ext_grid_idx, 'q_mvar']
        #     print(external_qgen)
        #
        #     zeroed = abs(external_qgen) < external_qgen_threshold
        #     step = 1
        #     while not zeroed and step < 120:
        #         zeroed = True
        #         # -- CALCULATE PARTICIPATING GENERATORS UP-DOWN P AND Q MARGINS -------
        #         q_upmargin_total = 0.0  # INITIALIZE TOTAL Q-UP MARGIN
        #         q_downmargin_total = 0.0  # INITIALIZE TOTAL Q-DOWN MARGIN
        #         q_upmargin_dict = {}  # INITIALIZE Q-UP MARGIN DICT
        #         q_downmargin_dict = {}  # INITIALIZE Q-DOWN MARGIN DICT
        #         for gkey in participating_gens:  # LOOP THROUGH PARTICIPATING GENERATORS
        #             gidx = gen_dict[gkey]  # GET THIS PARTICIPATING GENERATOR INDEX
        #             if gidx in swinggen_idxs:
        #                 continue
        #             qgen = gnet.res_gen.loc[gidx, 'q_mvar']  # THIS GENERATORS QGEN
        #             qmin = gnet.gen.loc[gidx, 'min_q_mvar']  # THIS GENERATORS QMIN
        #             qmax = gnet.gen.loc[gidx, 'max_q_mvar']  # THIS GENERATORS QMAX
        #             q_upmargin = qmax - qgen  # THIS GENERATORS Q-UP MARGIN
        #             q_downmargin = qgen - qmin  # THIS GENERATORS Q-DOWN MARGIN
        #             q_upmargin_dict.update({gidx: q_upmargin})  # UPDATE Q-UP MARGIN DICT
        #             q_upmargin_total += q_upmargin  # INCREMENT TOTAL Q-UP MARGIN
        #             q_downmargin_dict.update({gidx: q_downmargin})  # UPDATE Q-DOWN MARGIN DICT
        #             q_downmargin_total += q_downmargin  # INCREMENT TOTAL Q-DOWN MARGIN
        #
        #         if abs(external_qgen) > external_qgen_threshold:  # CHECK IF EXTERNAL REACTIVE POWER EXCEED THRESHOLD
        #             for gkey in participating_gens:  # LOOP THROUGH PARTICIPATING GENERATORS
        #                 gidx = gen_dict[gkey]  # GET THIS PARTICIPATING GENERATOR INDEX
        #                 if gidx in swinggen_idxs:
        #                     continue
        #
        #                 zeroed = False  # SET ZEROED FLAG
        #                 # if gidx in swinggen_idxs:  # CHECK IF SWING GENERATOR...
        #                 #     continue  # IF SWING GEN, GET NEXT GENERATOR
        #                 vreg = gnet.res_gen.loc[gidx, 'vm_pu']  # THIS GENERATORS VOLTAGE SETPOINT
        #                 if external_qgen < -external_qgen_threshold:  # CHECK IF EXTERNAL REACTIVE POWER IS NEGATIVE
        #                     q_downmargin = q_downmargin_dict[gidx]  # GET THIS GENERATORS Q-DOWN MARGIN
        #
        #                     print(gkey, q_downmargin, q_downmargin_total)
        #
        #                     if vreg < 0.901 or q_downmargin < 1.0:  # IF NO MARGIN, OR BUS VOLTAGE IS LOW...
        #                         continue  # IF SO, GET NEXT GENERATOR
        #                     delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total  # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
        #                     new_vreg = vreg + delta_vreg  # CALCULATE NEW SET POINT
        #                     gnet.gen.loc[gidx, 'vm_pu'] = new_vreg  # SET GENERATOR QGEN FOR THIS NETWORK
        #
        #
        #
        #                 # if external_qgen > external_qgen_threshold:  # CHECK IF EXTERNAL REACTIVE POWER IS POSITIVE
        #                 #     q_upmargin = q_upmargin_dict[gidx]  # GET THIS GENERATORS Q-UP MARGIN
        #                 #     if vreg > 1.099 or q_upmargin < 1.0:  # IF NO MARGIN, OR BUS VOLTAGE IS HIGH...
        #                 #         continue  # IF SO, GET NEXT GENERATOR
        #                 #     delta_vreg = 0.020 * external_qgen * q_upmargin_dict[gidx] / q_upmargin_total  # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
        #                 #     new_vreg = vreg + delta_vreg  # CALCULATE NEW SET POINT
        #                 #     gnet.gen.loc[gidx, 'vm_pu'] = new_vreg  # SET GENERATOR QGEN FOR THIS NETWORK
        #         pp.runpp(gnet, enforce_q_lims=True)  # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        #         external_qgen = gnet.res_ext_grid.loc[ext_grid_idx, 'q_mvar']  # GET EXTERNAL GRID REACTIVE POWER
        #         step += 1  # INCREMENT ITERATOR
        #         print(external_qgen)
        #
        #     raise Exception


        # -- ITERATE TO FIND OPTIMUM GENERATOR OUTAGE DELTA VARIABLE  ------------------------------
        step = 1                                                                                    # INITIALIZE ITERATOR
        delta = delta_init                                                                          # SET INITIAL DELTA VARIABL
        net = copy.deepcopy(gnet)                                                                   # GET COPY OF OUTAGED NETWORK
        while step < 120:                                                                           # LIMIT WHILE LOOPS
            for gen_pkey in participating_gens:                                                     # LOOP THROUGH PARTICIPATING GENERATORS
                if gen_pkey == genkey:                                                              # IF GENERATOR = OUTAGED GENERATOR
                    continue                                                                        # GET NEXT GENERATOR
                gidx = gen_dict[gen_pkey]                                                           # GET THIS GENERATOR INDEX
                pgen_a = base_pgens[gen_pkey]                                                       # THIS GENERATORS RATEA BASECASE PGEN
                pmin = net.gen.loc[gidx, 'min_p_mw']                                                # THIS GENERATORS PMIN
                pmax = net.gen.loc[gidx, 'max_p_mw']                                                # THIS GENERATORS MAX
                pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
                target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
                if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
                    net.gen.loc[gidx, 'p_mw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
                elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
                    net.gen.loc[gidx, 'p_mw'] = pmax                                                # SET PGEN = PMAX
                elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
                    net.gen.loc[gidx, 'p_mw'] = pmin                                                # SET PGEN = PMIN
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
            if abs(ex_pgen) < 1e-3:                                                                 # IF EXTERNAL GRID POWER IS NEAR ZERO..
                break                                                                               # BREAK AND GET NEXT GEN OUTAGE
            delta += ex_pgen / pfactor_total                                                        # INCREMENT DELTA
            step += 1                                                                               # INCREMENT ITERATION
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # GET EXTERNAL GRID REAL POWER
        ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                      # GET EXTERNAL GRID REACTIVE POWER
        print('GEN {0:5s}  . . . . . . . . . . . . . . . . . . . . . . . . . . . . .'.format(genkey),
              '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6))

        # ---------------------------------------------------------------------------------------------
        # -- WRITE GENERATOR CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------------------------
        # ---------------------------------------------------------------------------------------------
        bus_results = copy.deepcopy(net.res_bus)                                                        # GET CONTINGENCY BUS RESULTS
        gen_results = copy.deepcopy(net.res_gen)                                                        # GET CONTINGENCY GENERATOR RESULTS
        write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE SOLUTION2 BUS RESULTS
        write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE SOLUTION2 GENERATOR RESULTS
    print('DELTAS FOR GENERATOR OUTAGES ESTIMATED .............................', round(time.time() - gdelta_starttime, 3))
    print()

    # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- RUN LINE AND XFMR OUTAGES TO FIND OPTIMUM DELTA VARIABLE ---------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    bdelta_starttime = time.time()
    # cnet = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY NETWORK
    # for genbus in genidx_dict:                                                                    # LOOP ACROSS GENERATOR BUSES
    #     gidx = genidx_dict[genbus]                                                                # GET GENERATOR INDEX
    #     cnet.gen.loc[gidx, 'controllable'] = False                                                # TURN OFF GENERATOR OPF CONTROL (NOT SWSHUNTS)
    #
    # # -- TODO DOES OPTIMIZE SHUNTS GO BEFORE LAST DELTA ITERATION?
    # pp.runopp(cnet, enforce_q_lims=True)                                                          # RUN OPTIMAL POWER FLOW (OPTIMIZE SWSHUNTS)
    # for shbus in swshidx_dict:                                                                    # LOOP ACROSS SWSHUNT (GEN) BUSES
    #     shidx = swshidx_dict[shbus]                                                               # GET SWSHUNT INDEX
    #     cnet.gen.loc[shidx, 'vm_pu'] = cnet.res_bus.loc[shbus, 'vm_pu']                           # SET SWSHUNT VREG TO OPF RESULTS
    # pp.runpp(cnet, enforce_q_lims=True)                                                           # RUN STRAIGHT POWER FLOW

    pfactor_total = 0.0                                                                             # INITIALIZE PARTICIPATION FACTORS TOTAL
    for gen_pkey in pfactor_dict:                                                                   # LOOP ACROSS PARTICIPATING GENERATORS
        if gen_pkey not in online_gens:                                                             # CHECK IF PARTICIPATING GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT PARTICIPATING GENERATOR
        gidx = gen_dict[gen_pkey]                                                                   # GET PARTICIPATING GENERATOR INDEX
        pfactor_total += pfactor_dict[gen_pkey]                                                     # INCREMENT PFACTOR TOTAL

    btext = ''                                                                                      # DECLARE BRANCH TYPE TEXT
    for branchkey in boutagekeys:                                                                   # LOOP THROUGH BRANCH OUTAGES
        bnet = copy.deepcopy(net_c)                                                                 # INITIALIZE THIS CONTINGENCY NETWORK
        conlabel = "'" + outage_dict['branch'][branchkey] + "'"                                     # GET CONTINGENCY LABEL
        if branchkey in line_dict:                                                                  # CHECK IF BRANCH IS A LINE...
            btext = 'LINE'                                                                          # ASSIGN BRANCH TYPE TEXT
            lineidx = line_dict[branchkey]                                                          # GET LINE INDEX
            if not bnet.line.loc[lineidx, 'in_service']:                                            # CHECK IF OUTAGED LINE IS IN-SERVICE...
                continue                                                                            # IF NOT, GET NEXT BRANCH
            bnet.line.in_service[lineidx] = False                                                   # TAKE LINE OUT OF SERVICE
        elif branchkey in xfmr_dict:                                                                # CHECK IF BRANCH IS A XFMR...
            btext = 'XFMR'                                                                          # ASSIGN BRANCH TYPE TEXT
            xfmridx = xfmr_dict[branchkey]                                                          # GET XFMR INDEX
            if not bnet.trafo.loc[xfmridx, 'in_service']:                                           # CHECK IF OUTAGED XFMR IS IN-SERVICE...
                continue                                                                            # IF NOT, GET NEXT BRANCH
            bnet.trafo.in_service[xfmridx] = False                                                  # TAKE XFMR OUT OF SERVICE
        try:                                                                                        # TRY STRAIGHT POWER FLOW
            pp.runpp(bnet, enforce_q_lims=True)                                                     # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
        except:                                                                                     # IF NOT SOLVED... WRITE BASECASE RESULTS AS PLACEHOLDER
            delta = 0.0                                                                             # ASSIGN DELTA = 0
            bus_results = copy.deepcopy(net_a.res_bus)                                              # GET BASECASE BUS RESULTS
            gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
            write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE DUMMY BUS RESULTS TO SOLUTION2 FILE
            write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE DUMMY BUS RESULTS TO SOLUTION2 FILE
            continue                                                                                # GET NEXT BRANCH

        # -- ITERATE TO FIND OPTIMUM BRANCH OUTAGE DELTA VARIABLE  ---------------------------------
        step = 1                                                                                    # INITIALIZE ITERATOR
        delta = 0.0                                                                                 # INITIALIZE DELTA VARIABLE
        net = copy.deepcopy(bnet)                                                                   # GET FRESH COPY INITIALIZED NETWORK
        while step < 120:                                                                           # LIMIT WHILE LOOPS
            for gen_pkey in participating_gens:                                                     # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gen_pkey]                                                           # GET THIS GENERATOR INDEX
                pgen_a = base_pgens[gen_pkey]                                                       # THIS GENERATORS RATEA BASECASE PGEN
                pmin = net.gen.loc[gidx, 'min_p_mw']                                                # THIS GENERATORS PMIN
                pmax = net.gen.loc[gidx, 'max_p_mw']                                                # THIS GENERATORS MAX
                pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
                target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
                if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
                    net.gen.loc[gidx, 'p_mw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
                elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
                    net.gen.loc[gidx, 'p_mw'] = pmax                                                # SET PGEN = PMAX
                elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
                    net.gen.loc[gidx, 'p_mw'] = pmin                                                # SET PGEN = PMIN
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
            if abs(ex_pgen) < 1e-3:                                                                 # IF EXTERNAL GRID POWER IS NEAR ZERO..
                break                                                                               # BREAK AND GET NEXT GEN OUTAGE
            delta += ex_pgen / pfactor_total                                                        # INCREMENT DELTA
            step += 1                                                                               # INCREMENT ITERATION

        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # GET EXTERNAL GRID REAL POWER
        ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                      # GET EXTERNAL GRID REACTIVE POWER
        print('{0:4s} {1:9s} . . . . . . . . . . . . . . . . . . . . . . . . . . . '.format(btext, branchkey),
              '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6))

        # ---------------------------------------------------------------------------------------------
        # -- WRITE BRANCH CONTINGENCY BUS AND GENERATOR RESULTS TO FILE -------------------------------
        # ---------------------------------------------------------------------------------------------
        bus_results = copy.deepcopy(net.res_bus)                                                        # GET CONTINGENCY BUS RESULTS
        gen_results = copy.deepcopy(net.res_gen)                                                        # GET CONTINGENCY GENERATOR RESULTS
        write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE SOLUTION2 BUS RESULTS
        write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE SOLUTION2 GENERATOR RESULTS
    print('DELTAS FOR LINE AND XFMR OUTAGES ESTIMATED .........................', round(time.time() - bdelta_starttime, 3))
    print()
